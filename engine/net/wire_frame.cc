// 编解码协议版本一的实时信封，统一两种公网传输和内网回执的字段检查。
#include "engine/net/wire_frame.h"

#include <chrono>
#include <utility>

#include "engine/core/log.h"

namespace pop {
namespace {
// 第一版实时信封的协议登记号。
constexpr unsigned PROTOCOL_ID = 1;
}  // namespace

WireBytes EncodeFrame(wire::MessageType type, std::string payload) {
  if (payload.size() > MAX_REALTIME_FRAME_BYTES) {
    return {};
  }
  wire::Envelope envelope;
  envelope.set_protocol_id(PROTOCOL_ID);
  envelope.set_message_type(type);
  envelope.set_payload_length(payload.size());
  envelope.set_payload(std::move(payload));

  const auto size = envelope.ByteSizeLong();
  if (size > MAX_REALTIME_FRAME_BYTES) {
    return {};
  }
  WireBytes bytes(size);
  if (!envelope.SerializeToArray(bytes.data(), static_cast<int>(size))) {
    return {};
  }
  return bytes;
}

std::optional<wire::Envelope> DecodeFrame(const WireBytes& bytes) {
  wire::Envelope envelope;
  if (bytes.empty() || bytes.size() > MAX_REALTIME_FRAME_BYTES ||
      !envelope.ParseFromArray(bytes.data(), bytes.size()) ||
      envelope.protocol_id() != PROTOCOL_ID ||
      envelope.payload_length() != envelope.payload().size()) {
    return std::nullopt;
  }
  return envelope;
}

void ReplyToInput(const SendFrame& send, Id request, Id sequence, SpaceId space,
                  Tick tick, wire::InputStatus status) {
  wire::InputReceipt receipt;
  receipt.set_request_id(request);
  receipt.set_input_seq(sequence);
  receipt.set_space_id(space);
  receipt.set_server_tick(tick);
  receipt.set_status(status);
  const auto result =
      send(EncodeFrame(wire::INPUT_RECEIPT, receipt.SerializeAsString()), {});
  if (result != SendStatus::Queued) {
    LOG_INFO("event=receipt_undelivered request={} seq={} space={} tick={}",
             request, sequence, space, tick);
  } else if (request) {
    LOG_INFO(
        "event=input_receipt_queued request={} input_seq={} space={} "
        "tick={} status={}",
        request, sequence, space, tick, wire::InputStatus_Name(status));
  }
}

std::int64_t UnixMilliseconds() {
  return std::chrono::duration_cast<Milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace pop
