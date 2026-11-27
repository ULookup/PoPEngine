"""为本地协议验收生成临时证书，以明确的就绪日志同步进程启动和退出。"""

from collections import deque
from contextlib import contextmanager
from pathlib import Path
import queue
import signal
import socket
import ssl
import struct
import subprocess
import threading


def run_quic(driver, port, authority, request, *options):
    """以标准输入输出传递二进制信封，进程参数和异常诊断只显示连接选项。"""
    return subprocess.run(
        [driver, "127.0.0.1", str(port), authority, *options],
        input=request, capture_output=True, timeout=7,
    )


def issue_certificates(directory, names):
    """签发短期测试 CA 与各服务身份，可用同一身份测试双向 TLS。"""
    root = Path(directory)

    def openssl(*args):
        subprocess.run(
            ["openssl", *args], cwd=root, check=True, capture_output=True,
            timeout=10,
        )

    openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=PopEngine test CA", "-addext",
        "basicConstraints=critical,CA:TRUE", "-addext",
        "keyUsage=critical,keyCertSign", "-keyout", "ca.key", "-out", "ca.crt",
    )
    for serial, name in enumerate(names, start=2):
        openssl(
            "req", "-new", "-newkey", "rsa:2048", "-nodes",
            "-subj", f"/CN={name}", "-keyout", f"{name}.key",
            "-out", f"{name}.csr",
        )
        san = f"DNS:{name}"
        if name == "localhost":
            san += ",IP:127.0.0.1"
        (root / "leaf.ext").write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth,clientAuth\n"
            f"subjectAltName={san}\n"
        )
        openssl(
            "x509", "-req", "-days", "1", "-in", f"{name}.csr",
            "-CA", "ca.crt", "-CAkey", "ca.key", "-set_serial", str(serial),
            "-extfile", "leaf.ext", "-out", f"{name}.crt",
        )


def free_port():
    """取得可供当前隔离回环服务监听的临时 TCP 端口。"""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextmanager
def running_process(command, ready_marker, stop_signal=signal.SIGTERM):
    """等待明确的就绪行并返回该行；持续排空输出，最终检查退出码。"""
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    ready = queue.Queue(maxsize=1)
    recent = deque(maxlen=30)

    def collect():
        waiting = True
        for line in process.stdout:
            recent.append(line)
            if waiting and ready_marker in line:
                ready.put(line.rstrip())
                waiting = False
        if waiting:
            ready.put(None)

    reader = threading.Thread(target=collect, daemon=True)
    reader.start()
    try:
        try:
            line = ready.get(timeout=8)
        except queue.Empty as error:
            raise RuntimeError(f"readiness timeout: {command[0]}") from error
        if line is None:
            raise RuntimeError("process exited before ready: " + "".join(recent))
        yield line
    finally:
        if process.poll() is None:
            process.send_signal(stop_signal)
        try:
            code = process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise
        finally:
            reader.join(timeout=1)
            process.stdout.close()
        if code:
            raise RuntimeError(f"{command[0]} exited {code}: " + "".join(recent))


def running_role(binary, config):
    """从正式进程入口加载配置，返回含实例 ID 的完整 ready 事件。"""
    return running_process([binary, "--config", config], "event=ready")


def quoted(data):
    """用 Protobuf 文本格式表示任意二进制字段。"""
    return '"' + ''.join(f'\\{byte:03o}' for byte in data) + '"'


@contextmanager
def connect(root, port, hostname, identity=None):
    """连接到回环地址，验证对端名称，并按需提交客户端证书。"""
    context = ssl.create_default_context(cafile=str(root / "ca.crt"))
    if identity:
        context.load_cert_chain(root / f"{identity}.crt", root / f"{identity}.key")
    with socket.create_connection(("127.0.0.1", port), timeout=3) as raw:
        with context.wrap_socket(raw, server_hostname=hostname) as stream:
            yield stream


def framed(data):
    """给单个 Protobuf 信封加网络序长度前缀。"""
    return struct.pack("!I", len(data)) + data


def read_exact(stream, size):
    """在套接字截止时间内读满指定字节数。"""
    data = bytearray()
    while len(data) < size:
        part = stream.recv(size - len(data))
        if not part:
            raise EOFError("connection closed before a complete frame")
        data.extend(part)
    return bytes(data)
