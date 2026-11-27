"""沿 Gateway 的独立普通状态通道读取整步统计，并检查查询与观察连接的关联。"""

import ast
import struct
import time

from network_fixture import connect, framed, quoted, read_exact, run_quic


def verify_state(root, port, token, protocol, driver):
    """从同时在线的两个 TCP 客户端和一个 QUIC 客户端读取已提交快照。"""
    def request(query_id, credential=token):
        fields = f"fixture_token: {quoted(credential)} query_id: {query_id}"
        return protocol.envelope("SampleStateQuery", "SAMPLE_STATE_QUERY", fields)

    def decode(data):
        envelope = protocol.decode("Envelope", data)
        if (
            envelope.get("protocol_id") != "1"
            or envelope.get("message_type") != "PUBLISHED_STATE"
        ):
            raise RuntimeError(f"unexpected state envelope: {envelope}")
        payload = ast.literal_eval("b" + envelope["payload"])
        if int(envelope["payload_length"]) != len(payload):
            raise RuntimeError("wrong published state length")
        return protocol.decode("PublishedState", payload)

    def receive(connection):
        length, = struct.unpack("!I", read_exact(connection, 4))
        if not 0 < length <= 65536:
            raise RuntimeError(f"invalid state frame length: {length}")
        return decode(read_exact(connection, length))

    def expect(state, query_id):
        if (
            state.get("status") != "STATE_AVAILABLE"
            or state.get("query_id") != str(query_id)
            or state.get("space_id") != "101"
            or state.get("entities") != "1"
            or int(state.get("inputs", 0)) < 3
            or int(state.get("server_tick", 0)) < 1
        ):
            raise RuntimeError(f"unexpected published state for {query_id}: {state}")

    with connect(root, port, "localhost") as first, connect(
        root, port, "localhost"
    ) as second:
        deadline = time.monotonic() + 4
        while True:
            first.sendall(framed(request(91)))
            state = receive(first)
            if state.get("status") != "STATE_BUSY":
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("ordinary state channel did not become ready")
        expect(state, 91)

        # 两条连接上的同号请求都应收到回复；随后用不同编号检查回送对象。
        for left, right in [(92, 92), (93, 94)]:
            left_frame = framed(request(left))
            right_frame = framed(request(right))
            first.sendall(left_frame)
            second.sendall(right_frame)
            expect(receive(first), left)
            expect(receive(second), right)

        second.sendall(framed(request(95, bytes(32))))
        rejected = receive(second)
        if rejected.get("status") != "STATE_REJECTED" or "entities" in rejected:
            raise RuntimeError(f"invalid credential disclosed state: {rejected}")

    result = run_quic(driver, port, root / "ca.crt", request(96), "datagram")
    result.check_returncode()
    expect(decode(result.stdout), 96)
