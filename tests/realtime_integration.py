"""以临时证书和预置实体经真实 Gateway、Worker 验收 TCP/TLS、mTLS 与 QUIC。"""

import ast
import json
from pathlib import Path
import secrets
import ssl
import struct
import subprocess
import sys
import tempfile
import time

from network_fixture import (
    connect, framed, free_port, issue_certificates, quoted, read_exact,
    run_quic, running_role,
)
from process_lifecycle import worker_config
from state_channel_check import verify_state


SPACE = 101
BINDING = {
    "space_id": SPACE, "entity_id": 83, "entity_generation": 7,
    "session_epoch": 2,
}


def require(condition, message):
    """将预期与观察值一同报告给 CTest。"""
    if not condition:
        raise RuntimeError(message)


class Protocol:
    """通过同一份项目 Schema 调用 protoc，构造请求并解码真实回执。"""

    def __init__(self, compiler):
        self.compiler = compiler
        self.schema = Path(__file__).resolve().parents[1] / "proto"

    def convert(self, action, name, data):
        """在标准输入传送载荷；进程参数只包含类型和 Schema 名。"""
        return subprocess.run(
            [self.compiler, f"--{action}=pop.wire.{name}", "realtime.proto"],
            cwd=self.schema, input=data, capture_output=True,
            check=True, timeout=3,
        ).stdout

    def envelope(self, name, kind, fields):
        """将样例或内网批次编码成受正式入口校验的完整信封。"""
        payload = self.convert("encode", name, fields.encode())
        meta = (
            f"protocol_id: 1 message_type: {kind} "
            f"payload_length: {len(payload)} payload: {quoted(payload)}"
        )
        return self.convert("encode", "Envelope", meta.encode())

    def sample(self, sequence, token):
        """构造完整控制状态，客户端只提供凭据和会话内序号。"""
        return self.envelope("SampleInput", "SAMPLE_INPUT", (
            f'fixture_token: {quoted(token)} input_seq: {sequence} state: "x"'
        ))

    def batch(self, instance, expires):
        """构造绑定到指定 Worker 实例的可信内网输入。"""
        return self.envelope("InputBatch", "INPUT_BATCH", (
            f'space_id: {SPACE} space_owner_epoch: 1 worker_instance_id: "{instance}" '
            'inputs { request_id: 8901 entity_id: 83 entity_generation: 7 '
            f'session_epoch: 2 input_seq: 95 state: "y" expires_at_unix_ms: {expires} }}'
        ))

    def decode(self, name, data):
        """读取只含标量字段的验收结果；省略的字段依 proto3 缺省值处理。"""
        text = self.convert("decode", name, data).decode("ascii")
        return dict(line.split(": ", 1) for line in text.splitlines())

    def receipt(self, data):
        """验证信封类型及边界后解析场景回执。"""
        env = self.decode("Envelope", data)
        require(env.get("protocol_id") == "1", "wrong receipt protocol")
        require(env.get("message_type") == "INPUT_RECEIPT", "wrong receipt type")
        payload = ast.literal_eval("b" + env["payload"])
        require(len(payload) == int(env["payload_length"]), "wrong payload length")
        return self.decode("InputReceipt", payload)


def read_receipt(stream, protocol):
    """通过真实 TLS 流取得一条有界回执。"""
    size, = struct.unpack("!I", read_exact(stream, 4))
    require(0 < size <= 65536, f"bad receipt size {size}")
    return protocol.receipt(read_exact(stream, size))


def expect(receipt, sequence, status):
    """关联序号及返回状态，失败时显示实际字段。"""
    require(receipt.get("input_seq") == str(sequence), f"wrong sequence: {receipt}")
    require(receipt.get("status", "UNKNOWN") == status, f"expected {status}: {receipt}")
    if status == "APPLIED":
        require(receipt.get("space_id") == str(SPACE), f"wrong Space: {receipt}")
        require(int(receipt.get("request_id", 0)) > 0, f"missing request: {receipt}")
        require(int(receipt.get("server_tick", 0)) > 0, f"missing Tick: {receipt}")


def quic_or_tcp(root, port, request, protocol, driver, *options):
    """QUIC 明确报出握手不可用且请求尚未发送时，沿用原帧建立兼容连接。"""
    result = run_quic(driver, port, root / "ca.crt", request, *options)
    if result.returncode == 3:
        with connect(root, port, "localhost") as peer:
            peer.sendall(framed(request))
            return "TCP/TLS", read_receipt(peer, protocol)
    require(result.returncode == 0,
            f"QUIC driver failed, exit={result.returncode}, stderr={result.stderr!r}")
    return "QUIC", protocol.receipt(result.stdout)


def verify_public(root, port, token, protocol, driver):
    """观察分片 TCP 输入及 QUIC 输入的有序提交，确认错误连接与正常连接隔离。"""
    with connect(root, port, "localhost") as stream:
        frame = framed(protocol.sample(1, token))
        deadline = time.monotonic() + 4
        while True:
            # 分别写出不完整前缀、剩余前缀与部分正文、剩余正文。
            for part in (frame[:1], frame[1:7], frame[7:]):
                stream.sendall(part)
            first = read_receipt(stream, protocol)
            if first.get("status") != "BUSY":
                break
            require(time.monotonic() < deadline, "no live Gateway to Scene link")
        expect(first, 1, "APPLIED")

        # 同一次写入包含两个完整帧。不同拒绝原因可能按不同顺序返回。
        stream.sendall(frame + framed(protocol.sample(50, bytes(32))))
        receipts = [read_receipt(stream, protocol) for _ in range(2)]
        by_sequence = {record["input_seq"]: record for record in receipts}
        expect(by_sequence["1"], 1, "STALE")
        expect(by_sequence["50"], 50, "REJECTED")

        request = protocol.sample(2, token)
        denied = run_quic(driver, port, root / "gateway.crt", request)
        require(
            denied.returncode == 1 and b"QUIC transport closed" in denied.stderr,
            f"expected QUIC certificate rejection, exit={denied.returncode}, "
            f"stderr={denied.stderr!r}",
        )
        transport, second = quic_or_tcp(root, port, request, protocol, driver)
        require(transport == "QUIC", f"DATAGRAM negotiation chose {transport}")
        expect(second, 2, "APPLIED")
        require(int(second["server_tick"]) > int(first["server_tick"]),
                f"QUIC receipt did not advance: {first} -> {second}")

        with connect(root, port, "localhost") as oversized:
            oversized.sendall(struct.pack("!I", 65537))
            try:
                require(oversized.recv(1) == b"", "oversize length was accepted")
            except (ConnectionResetError, ssl.SSLError):
                pass

        stream.sendall(framed(protocol.sample(3, token)))
        expect(read_receipt(stream, protocol), 3, "APPLIED")

        transport, fallback = quic_or_tcp(
            root, port, protocol.sample(4, token), protocol, driver, "no-datagram",
        )
        require(transport == "TCP/TLS", f"missing DATAGRAM chose {transport}")
        expect(fallback, 4, "APPLIED")
        print(f"TCP/TLS seq=1 tick={first['server_tick']}, "
              f"QUIC seq=2 tick={second['server_tick']}, space={SPACE}")


def verify_internal(root, port, instance, protocol):
    """验证 mTLS 区分同 CA 的服务身份，并检查在途消息的承载代次与期限。"""
    expired = framed(protocol.batch(instance, 1))
    try:
        with connect(root, port, "worker", identity="localhost") as peer:
            peer.sendall(expired)
            require(not peer.recv(1), "wrong mTLS role received a Scene reply")
    except (ssl.SSLError, ConnectionResetError, BrokenPipeError):
        pass

    with connect(root, port, "worker", identity="gateway") as peer:
        unsupported = protocol.convert(
            "encode", "Envelope", b"protocol_id: 1 message_type: 91"
        )
        peer.sendall(framed(unsupported))
        status = read_receipt(peer, protocol).get("status")
        require(status == "UNSUPPORTED", f"unknown internal message type: {status}")

        peer.sendall(expired)
        receipt = read_receipt(peer, protocol)
        expect(receipt, 95, "EXPIRED")
        require(receipt.get("request_id") == "8901", f"wrong correlation: {receipt}")

        peer.sendall(framed(protocol.batch(instance + "-old", 1)))
        expect(read_receipt(peer, protocol), 95, "STALE_ROUTE")


def tls_files(root, identity, peer_name):
    """为一个受信角色配置私钥、证书链及预期的对端服务名。"""
    return {
        "ca": str(root / "ca.crt"),
        "key": str(root / f"{identity}.key"),
        "certificate": str(root / f"{identity}.crt"),
        "peer_name": peer_name,
    }


def run(root, worker, gateway, driver, queue_driver, compiler):
    """分别给 Worker 和 Gateway 安装同一隔离绑定，启动后从公网入口验收。"""
    issue_certificates(root, ["localhost", "gateway", "worker"])
    token = secrets.token_bytes(32)
    token_file = root / "fixture.token"
    token_file.write_bytes(token)

    scene = {"host": "127.0.0.1", "port": free_port()}
    config = worker_config([{"space_id": SPACE, "scene_shard": 0}])
    config["network"] = {
        "scene": scene, "tls": tls_files(root, "worker", "gateway"),
        "isolated_fixture": BINDING,
    }
    path = root / "worker.yaml"
    path.write_text(json.dumps(config))
    protocol = Protocol(compiler)
    with running_role(worker, path) as ready:
        fields = dict(part.split("=", 1) for part in ready.split() if "=" in part)
        instance = fields["worker_instance_id"]
        ingress = {"host": "127.0.0.1", "port": free_port()}
        path = root / "gateway.yaml"
        path.write_text(json.dumps({
            "role": "gateway", "log_entries": 128,
            "network": {
                "ingress": ingress, "scene": scene,
                "tls": tls_files(root, "gateway", "worker"),
                "public_tls": {
                    "key": str(root / "localhost.key"),
                    "certificate": str(root / "localhost.crt"),
                },
                "isolated_fixture": {
                    **BINDING, "worker_instance_id": instance,
                    "token_file": str(token_file),
                },
            },
        }))
        with running_role(gateway, path):
            verify_public(root, ingress["port"], token, protocol, driver)
            verify_state(root, ingress["port"], token, protocol, driver)
            verify_internal(root, scene["port"], instance, protocol)

    subprocess.run(
        [queue_driver, str(free_port()), root / "localhost.crt",
         root / "localhost.key", root / "ca.crt", "localhost"],
        check=True, timeout=10,
    )


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temporary:
        run(Path(temporary), *map(Path, sys.argv[1:]))
