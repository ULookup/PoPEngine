"""用临时证书、回环端口和正式进程验证 Nginx 到 Platform 与 Control 的 mTLS RPC。"""

import http.client
import json
from pathlib import Path
import signal
import ssl
import subprocess
import sys
import tempfile

from network_fixture import (
    free_port, issue_certificates, running_process, running_role,
)


def write_config(root, role, network):
    """生成实际进程入口接受的独立角色配置。"""
    path = root / f"{role}.yaml"
    path.write_text(json.dumps({
        "role": role,
        "log_entries": 64,
        "network": network,
    }))
    return path


def verify_http(root, platform, nginx, template):
    """启动真实 Nginx HTTPS 前端并检查由 Beast 生成的 Platform 就绪结果。"""
    platform_port = free_port()
    config = write_config(root, "platform", {
        "http": {"host": "127.0.0.1", "port": platform_port},
    })
    issue_certificates(root, ["localhost"])
    with running_role(platform, config):
        https_port = free_port()
        conf = template.read_text()
        for name, value in {
            "HTTPS_PORT": https_port,
            "PLATFORM_PORT": platform_port,
            "CERTIFICATE": root / "localhost.crt",
            "PRIVATE_KEY": root / "localhost.key",
        }.items():
            conf = conf.replace(f"@{name}@", str(value))
        path = root / "nginx.conf"
        path.write_text(conf)

        with running_process(
            [nginx, "-p", str(root) + "/", "-c", path],
            "start worker process ", signal.SIGQUIT,
        ):
            context = ssl.create_default_context(cafile=str(root / "ca.crt"))
            connection = http.client.HTTPSConnection(
                "127.0.0.1", https_port, context=context, timeout=3,
            )
            try:
                connection.request("GET", "/healthz")
                response = connection.getresponse()
                payload = json.load(response)
                if response.status != 200 or payload != {
                    "status": "serving", "role": "platform",
                }:
                    raise RuntimeError(
                        f"unexpected health response {response.status} {payload}"
                    )
                connection.request("POST", "/healthz")
                rejected = connection.getresponse()
                rejected.read()
                if rejected.status != 405 or rejected.getheader("Allow") != "GET":
                    raise RuntimeError("health endpoint accepted a write")
                connection.request("GET", "/unknown")
                missing = connection.getresponse()
                missing.read()
                if missing.status != 404:
                    raise RuntimeError("unconfigured HTTPS route unexpectedly exists")
            finally:
                connection.close()


def verify_rpc(root, control, driver):
    """由一个已授权和一个同 CA 未授权的客户端检查 Control 的真实 RPC。"""
    issue_certificates(root, ["localhost", "platform", "unrelated"])
    port = free_port()
    config = write_config(root, "control", {
        "rpc": {"host": "127.0.0.1", "port": port},
        "tls": {
            "ca": str(root / "ca.crt"),
            "certificate": str(root / "localhost.crt"),
            "key": str(root / "localhost.key"),
            "peer_name": "platform",
        },
        "limits": {
            "max_connections": 16, "queue_entries": 1,
            "queue_bytes": 262144, "timeout_ms": 3000,
        },
    })
    with running_role(control, config):
        for identity, expected in (("platform", "serving"), ("unrelated", "denied")):
            subprocess.run(
                [driver, f"127.0.0.1:{port}", root / "ca.crt",
                 root / f"{identity}.crt", root / f"{identity}.key", expected],
                check=True, timeout=6,
            )


if __name__ == "__main__":
    if sys.argv[1] == "http" and sys.argv[3].endswith("-NOTFOUND"):
        print("PlatformHttpsHealth requires nginx (POP_NGINX_EXECUTABLE)")
        sys.exit(77)
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        if sys.argv[1] == "rpc":
            verify_rpc(root, *map(Path, sys.argv[2:]))
        else:
            verify_http(root, *map(Path, sys.argv[2:]))
