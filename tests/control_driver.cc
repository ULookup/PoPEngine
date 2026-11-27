// 用临时客户证书访问真实 Control，检查就绪、截止时间、并发名额释放和对端授权。
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

#include "control.grpc.pb.h"

namespace {

using namespace std::chrono_literals;
using grpc::health::v1::Health;
using grpc::health::v1::HealthCheckRequest;
using grpc::health::v1::HealthCheckResponse;

// 从临时验收目录读取 PEM 输入，仅将内容交给 gRPC 凭据接口。
std::string ReadFile(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    throw std::runtime_error("cannot read test TLS identity");
  }
  return {std::istreambuf_iterator<char>(file), {}};
}

// 使用明确的调用时限发起一次只读查询，并检查业务结果。
grpc::Status Check(Health::Stub& stub, const HealthCheckRequest& request,
                   std::chrono::milliseconds timeout) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + timeout);
  HealthCheckResponse result;
  auto status = stub.Check(&context, request, &result);
  if (status.ok() && result.status() != HealthCheckResponse::SERVING) {
    throw std::runtime_error("Control did not report SERVING");
  }
  return status;
}

// Watch 的首个回复证明流已建立；达到截止后同一连接的下一次查询可使用该名额。
void CheckDeadline(Health::Stub& stub, const HealthCheckRequest& request) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 800ms);
  auto watch = stub.Watch(&context, request);
  HealthCheckResponse result;
  if (!watch->Read(&result) ||
      result.status() != HealthCheckResponse::SERVING) {
    throw std::runtime_error("Watch did not return the initial health state");
  }
  if (Check(stub, request, 150ms).error_code() !=
      grpc::StatusCode::DEADLINE_EXCEEDED) {
    throw std::runtime_error("additional RPC bypassed single-stream budget");
  }
  if (watch->Read(&result) ||
      watch->Finish().error_code() != grpc::StatusCode::DEADLINE_EXCEEDED) {
    throw std::runtime_error("Watch did not finish at its deadline");
  }
  if (!Check(stub, request, 3s).ok()) {
    throw std::runtime_error("expired Watch did not release its stream");
  }
}

// 客户端主动取消观察时也要归还流名额，供随后的查询在原连接上复用。
void CheckCancellation(Health::Stub& stub, const HealthCheckRequest& request) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  auto watch = stub.Watch(&context, request);
  HealthCheckResponse initial;
  if (!watch->Read(&initial)) {
    throw std::runtime_error("Watch closed before client cancellation");
  }
  context.TryCancel();
  if (watch->Finish().error_code() != grpc::StatusCode::CANCELLED ||
      !Check(stub, request, 3s).ok()) {
    throw std::runtime_error("cancelled Watch did not release its stream");
  }
}

}  // namespace

// 按测试用例给定的目标与证书验证 mTLS 控制查询，失败时向 CTest 输出原因。
int main(int argc, char** argv) {
  try {
    if (argc != 6) {
      throw std::invalid_argument(
          "expected target ca certificate key scenario");
    }
    grpc::SslCredentialsOptions tls;
    tls.pem_root_certs = ReadFile(argv[2]);
    tls.pem_cert_chain = ReadFile(argv[3]);
    tls.pem_private_key = ReadFile(argv[4]);
    auto channel = grpc::CreateChannel(argv[1], grpc::SslCredentials(tls));
    auto stub = Health::NewStub(channel);
    HealthCheckRequest request;
    request.set_service("WorldCoordinator");

    const auto result = Check(*stub, request, 3s);
    if (std::string(argv[5]) == "denied") {
      if (result.error_code() != grpc::StatusCode::PERMISSION_DENIED) {
        throw std::runtime_error("unexpected result for unauthorized client");
      }
    } else {
      if (!result.ok()) {
        throw std::runtime_error("health check failed: " +
                                 result.error_message());
      }
      CheckDeadline(*stub, request);
      CheckCancellation(*stub, request);
      request.set_service("unregistered");
      if (Check(*stub, request, 3s).error_code() != grpc::StatusCode::NOT_FOUND) {
        throw std::runtime_error("unknown service unexpectedly reported healthy");
      }
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
