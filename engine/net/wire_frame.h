// 校验实时 Protobuf 信封并生成回执。长度前缀由各流传输适配器管理。
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "engine/net/transport.h"
#include "realtime.pb.h"

namespace pop {

// 控制状态是完整的小型输入，一个报文在 QUIC 和兼容路径上含义相同。
inline constexpr std::size_t MAX_CONTROL_BYTES = 768;
// 每份内网批次最多投递的实体输入数，限制单次收包回调的工作量。
inline constexpr int MAX_INPUTS_PER_BATCH = 64;
// 同一输入从可信 Gateway 收包起最多生效 250 毫秒。
inline constexpr Milliseconds INPUT_LIFETIME{250};

// 接管已编码的载荷并加上实时信封，超过帧上限或序列化失败时返回空帧。
WireBytes EncodeFrame(wire::MessageType type, std::string payload);

// 拒绝空帧、超长帧、未知协议或载荷长度不符；消息类型由各入口校验。
std::optional<wire::Envelope> DecodeFrame(const WireBytes& bytes);

// 填充输入关联字段并向原连接提交可靠回执。
void ReplyToInput(const SendFrame& send, Id request, Id sequence, SpaceId space,
                  Tick tick, wire::InputStatus status);

// 跨主机传递网关采样的截止时间；本地入队后等待与有效期使用单调时钟。
std::int64_t UnixMilliseconds();

}  // namespace pop
