// 用 gRPC 自带的 Health 服务报告 WorldCoordinator 的就绪状态，服务端强制 mTLS。
#include "engine/net/control_endpoints.h"

#include <grpc/grpc_security_constants.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/security/auth_metadata_processor.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace pop {
namespace {

// 读取启动凭据，错误消息仅描述配置失败，不包含证书或密钥内容。
std::string ReadPem(const std::string& path) {
  std::ifstream file(path, std::ios::binary);
  std::string pem{std::istreambuf_iterator<char>(file), {}};
  if (pem.empty() || file.bad()) {
    throw std::runtime_error("cannot read Control TLS credential");
  }
  return pem;
}

// 从 gRPC 已验证的证书 SAN 中匹配配置的调用方，限制同一 CA 下的服务权限。
class PeerIdentity final : public grpc::AuthMetadataProcessor {
 public:
  // 保存允许访问 Control 的对端身份。
  explicit PeerIdentity(std::string name) : m_name(std::move(name)) {}

  // 身份检查仅读取连接认证上下文，不需要阻塞线程。
  bool IsBlocking() const override { return false; }

  // 证书链通过 TLS 验证后，逐个检查完整 SAN，防止角色混用。
  grpc::Status Process(const InputMetadata&, grpc::AuthContext* context,
                       OutputMetadata*, OutputMetadata*) override {
    for (const auto& san :
         context->FindPropertyValues(GRPC_X509_SAN_PROPERTY_NAME)) {
      if (std::string_view(san.data(), san.size()) == m_name) {
        return grpc::Status::OK;
      }
    }
    return {grpc::StatusCode::PERMISSION_DENIED, "unknown control peer"};
  }

 private:
  std::string m_name;  // 调用方证书中必须出现的身份。
};

// Control 进程拥有独立的 gRPC 运行时与完成队列，停止时取消仍在观察的 RPC。
class ControlRpc final : public NetworkListener {
 public:
  // 配置双向证书认证和每连接并发预算，再暴露标准 Check 与 Watch 端点。
  ControlRpc(const Endpoint& endpoint, const TlsFiles& files,
             const NetLimits& limits) {
    if (files.m_peerName.empty() || limits.m_queueEntries == 0 ||
        limits.m_maxConnections == 0 || limits.m_queueBytes == 0) {
      throw std::invalid_argument("missing Control peer identity or budget");
    }

    grpc::SslServerCredentialsOptions tls(
        GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
    tls.pem_root_certs = ReadPem(files.m_ca);
    tls.pem_key_cert_pairs.push_back(
        {ReadPem(files.m_key), ReadPem(files.m_certificate)});
    auto credentials = grpc::SslServerCredentials(tls);
    credentials->SetAuthMetadataProcessor(
        std::make_shared<PeerIdentity>(files.m_peerName));

    // gRPC 的默认健康服务开关是进程级配置，首次创建 Control 时设置一次。
    static std::once_flag enable_health;
    std::call_once(enable_health,
                   [] { grpc::EnableDefaultHealthCheckService(true); });
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(4096);
    builder.SetMaxSendMessageSize(4096);
    builder.AddChannelArgument(
        GRPC_ARG_MAX_CONCURRENT_STREAMS,
        static_cast<int>(std::min<std::size_t>(
            limits.m_queueEntries, std::numeric_limits<int>::max())));
    grpc::ResourceQuota quota;
    if (limits.m_queueBytes >
        std::numeric_limits<std::size_t>::max() / limits.m_maxConnections) {
      throw std::invalid_argument("Control RPC memory budget overflow");
    }
    quota.Resize(limits.m_queueBytes * limits.m_maxConnections);
    builder.SetResourceQuota(quota);

    const auto host = endpoint.m_host.find(':') == std::string::npos
                          ? endpoint.m_host
                          : "[" + endpoint.m_host + "]";
    builder.AddListeningPort(host + ":" + std::to_string(endpoint.m_port),
                             credentials);
    m_queue = builder.AddCompletionQueue();
    m_server = builder.BuildAndStart();
    if (!m_server) {
      m_queue->Shutdown();
      drain();
      throw std::runtime_error("cannot start Control gRPC listener");
    }
    m_server->GetHealthCheckService()->SetServingStatus("WorldCoordinator",
                                                        true);
    m_thread = std::thread([this] { drain(); });
  }

  // 取消长时间进行的 Watch，回收 gRPC 的传输及轮询线程。
  ~ControlRpc() override {
    m_server->GetHealthCheckService()->Shutdown();
    m_server->Shutdown(std::chrono::system_clock::now());
    m_queue->Shutdown();
    m_thread.join();
  }

 private:
  // 驱动服务端传输完成队列直到退出，Health 请求本身由 gRPC 内置服务处理。
  void drain() {
    void* tag;
    bool ok;
    while (m_queue->Next(&tag, &ok)) {
    }
  }

  std::unique_ptr<grpc::ServerCompletionQueue> m_queue;  // 传输完成事件。
  std::unique_ptr<grpc::Server> m_server;                // Control RPC 监听器。
  std::thread m_thread;                                  // 完成队列的轮询线程。
};

}  // namespace

std::unique_ptr<NetworkListener> StartControlRpc(Endpoint endpoint,
                                                 TlsFiles files,
                                                 NetLimits limits) {
  return std::make_unique<ControlRpc>(endpoint, files, limits);
}

}  // namespace pop
