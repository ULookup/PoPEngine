"""在独立 Docker 私网通过 MySQL Router、RabbitMQ 与三主 Redis Cluster 验收阶段三。"""

from contextlib import ExitStack
import os
from pathlib import Path
import secrets
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from network_fixture import free_port


ROOT = Path(__file__).resolve().parents[1]


def retry(description, probe, timeout=45):
    """以有界轮询等待数据库、队列或集群报告明确状态。"""
    deadline = time.monotonic() + timeout
    while True:
        if probe():
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"等待{description}超时")
        time.sleep(0.15)


def call(args, *, env=None, data=None, check=True, timeout=25):
    """只将命令输出作为诊断，连接密码通过环境或临时配置传递。"""
    result = subprocess.run(args, input=data, text=True, capture_output=True,
                            env=env, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} exited {result.returncode}: "
                           f"{result.stderr[-1500:]}{result.stdout[-1500:]}")
    return result


def stop_process(process):
    """先关闭输入，随后有界回收 Router 或仍在等待事件的子进程。"""
    if process.stdin and not process.stdin.closed:
        try:
            process.stdin.close()
        except BrokenPipeError:
            pass
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
    if process.stdout:
        process.stdout.close()


def wait_line(process, marker, timeout=12):
    """从同一子进程的管道等待完整事件行，保留最后的输出协助定位失败。"""
    deadline = time.monotonic() + timeout
    output = b""
    while marker not in output.split(b"\n")[:-1]:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
            raise RuntimeError(f"等待 {marker!r} 超时: {output[-1000:]!r}")
        chunk = os.read(process.stdout.fileno(), 4096)
        if not chunk:
            raise RuntimeError(f"未观察到 {marker!r}: {output[-1000:]!r}")
        output = (output + chunk)[-16384:]
    return output.decode(errors="replace").splitlines()


def router_endpoint(stack, root, mysql_host):
    """使用指定 MySQL Router 的静态主写路由，只监听本机临时端口。"""
    requested = os.environ.get("POP_MYSQLROUTER") or shutil.which("mysqlrouter")
    if not requested:
        raise RuntimeError("需要 mysqlrouter，或通过 POP_MYSQLROUTER 指定可执行文件")
    binary = Path(requested).resolve()
    prefix = binary.parents[1]
    plugins = prefix / "lib/mysqlrouter/plugin"
    libraries = prefix / "lib/mysql-router"
    env = os.environ.copy()
    if libraries.is_dir():
        env["LD_LIBRARY_PATH"] = os.pathsep.join(
            [str(libraries), *filter(None, [env.get("LD_LIBRARY_PATH")])])
    port = free_port()
    config = root / "mysqlrouter.conf"
    plugin_folder = f"plugin_folder={plugins}\n" if plugins.is_dir() else ""
    config.write_text(
        f"[DEFAULT]\nlogging_folder={root}\nruntime_folder={root}\n"
        f"data_folder={root}\n{plugin_folder}\n[routing:sample]\n"
        f"bind_address=127.0.0.1\nbind_port={port}\n"
        f"destinations={mysql_host}:3306\nrouting_strategy=first-available\n"
        "protocol=classic\n"
    )
    log = stack.enter_context((root / "router-startup.log").open("w+"))
    process = subprocess.Popen([binary, "-c", config], env=env,
                               stdout=log, stderr=subprocess.STDOUT)
    stack.callback(stop_process, process)

    def connected():
        """从 Router 读取经典协议握手，确认它已接通主写端点。"""
        if process.poll() is not None:
            log.seek(0)
            raise RuntimeError("MySQL Router 启动失败: " + log.read()[-1000:])
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as sock:
                greeting = sock.recv(5, socket.MSG_WAITALL)
                return len(greeting) == 5 and greeting[4] == 10
        except OSError:
            return False

    retry("MySQL Router 转发主库握手", connected, timeout=12)
    return port


def run_fixture(driver, root, stack, passwords):
    """构建隔离依赖；锁等待、离线投递、恢复消费和清空缓存均经正式驱动入口。"""
    prefix = "pop-phase3-" + uuid.uuid4().hex[:16]
    network = prefix + "-net"
    call(["docker", "network", "create", "--internal", network])
    stack.callback(call, ["docker", "network", "rm", network])

    def container(role, image, options=(), command=(), env=None):
        """在本轮私网内启动一个容器并登记卷和容器的退出清理。"""
        name = prefix + "-" + role
        call(["docker", "run", "-d", "--name", name, "--network", network,
              *options, image, *command], env=env, timeout=120)
        stack.callback(call, ["docker", "rm", "-fv", name])
        return name

    def address(name):
        """取得当前容器在本轮单独桥接网络中的地址。"""
        expression = "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
        return call(["docker", "inspect", "-f", expression, name]).stdout.strip()

    admin_secret, db_secret, mq_secret, redis_secret = passwords
    env = dict(os.environ, MYSQL_ROOT_PASSWORD=admin_secret, MYSQL_USER="sample",
               MYSQL_PASSWORD=db_secret, MYSQL_DATABASE="sample",
               RABBITMQ_DEFAULT_USER="sample", RABBITMQ_DEFAULT_PASS=mq_secret,
               RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="+S 2:2",
               RABBITMQ_CTL_ERL_ARGS="+S 1:1", REDISCLI_AUTH=redis_secret)
    db = container("db", "mysql:8.0.44",
                   ["-e", "MYSQL_ROOT_PASSWORD", "-e", "MYSQL_USER",
                    "-e", "MYSQL_PASSWORD", "-e", "MYSQL_DATABASE"], env=env)
    broker = container("mq", "rabbitmq:3.12.1",
                       ["-e", "RABBITMQ_DEFAULT_USER", "-e", "RABBITMQ_DEFAULT_PASS",
                        "-e", "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS",
                        "-e", "RABBITMQ_CTL_ERL_ARGS"], env=env)
    redis_config = root / "redis.conf"
    redis_config.write_text(
        "bind 0.0.0.0\ncluster-enabled yes\ncluster-node-timeout 3000\n"
        f"requirepass {redis_secret}\nmasterauth {redis_secret}\n"
    )
    redis_config.chmod(0o644)  # 临时目录仅当前用户可进入；容器 Redis 以专用 UID 读文件。
    nodes = [container(f"redis-{index}", "redis:7.2.5",
                       ["--mount", f"type=bind,src={redis_config},dst=/tmp/redis.conf,ro"],
                       ["redis-server", "/tmp/redis.conf"]) for index in range(3)]

    sql_env = dict(os.environ, MYSQL_PWD=admin_secret)
    mysql = ["docker", "exec", "-i", "-e", "MYSQL_PWD", db, "mysql",
             "--protocol=tcp", "-h127.0.0.1", "-uroot", "--batch", "--raw",
             "--skip-column-names", "--unbuffered", "sample"]

    def sql(statement, check=True):
        """在当前隔离库的管理会话中执行准备或观察语句。"""
        return call(mysql, env=sql_env, data=statement + "\n", check=check, timeout=6)

    def broker_ready():
        """确认重启后的节点已注册 AMQP 监听端口。"""
        return call(["docker", "exec", broker, "rabbitmq-diagnostics",
                     "-q", "check_port_listener", "5672"], check=False,
                    timeout=8).returncode == 0

    def redis(node, *args, check=True):
        """用测试 ACL 对指定独立容器执行 Redis 管理命令。"""
        return call(["docker", "exec", "-e", "REDISCLI_AUTH", node, "redis-cli",
                     "-e", *args], env=env, check=check)

    retry("MySQL 接受主库事务", lambda: sql("SELECT 1", False).stdout.strip() == "1",
          timeout=90)
    sql((ROOT / "storage/schema.sql").read_text()
        + "\nINSERT INTO sample_counter(aggregate_id) VALUES(42);")
    router_port = router_endpoint(stack, root, address(db))
    retry("RabbitMQ 监听 AMQP", broker_ready, timeout=60)
    for node in nodes:
        retry("Redis 节点就绪", lambda: redis(node, "PING", check=False).stdout.strip() == "PONG")
    redis_hosts = [address(node) for node in nodes]
    redis(nodes[0], "--cluster", "create", *[f"{host}:6379" for host in redis_hosts],
          "--cluster-replicas", "0", "--cluster-yes")
    for node in nodes:
        retry("Redis Cluster 槽位就绪",
              lambda: "cluster_state:ok" in redis(node, "CLUSTER", "INFO").stdout)

    driver_env = dict(os.environ, POP_DB_HOST="127.0.0.1", POP_DB_PORT=str(router_port),
                      POP_DB_USER="sample", POP_DB_PASSWORD=db_secret, POP_DB_NAME="sample",
                      POP_AMQP_URI=f"amqp://sample:{mq_secret}@{address(broker)}:5672/",
                      POP_REDIS_HOST=redis_hosts[0], POP_REDIS_PORT="6379",
                      POP_REDIS_PASSWORD=redis_secret,
                      POP_WORKER_CONFIG=str(ROOT / "configs/game_worker.yaml"))

    def drive(mode):
        """对外部驱动给出本次实例的地址、凭据和明确的等待上限。"""
        call([driver, mode], env=driver_env, timeout=40)
        print(f"{mode} passed", flush=True)

    drive("topology")
    call(["docker", "stop", "--time", "10", broker])
    with ExitStack() as processes:
        def interactive(command, environment):
            """打开可双向同步锁等待和 Scene 提交证据的子进程。"""
            process = subprocess.Popen(command, env=environment, stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            processes.callback(stop_process, process)
            return process

        lock = interactive(mysql, sql_env)
        lock.stdin.write(b"BEGIN; SELECT value INTO @held FROM sample_counter "
                         b"WHERE aggregate_id=42 FOR UPDATE; SELECT 'LOCK_HELD';\n")
        lock.stdin.flush()
        wait_line(lock, b"LOCK_HELD")
        writer = interactive([driver, "write"], driver_env)

        def writer_waits():
            """读取 InnoDB 真实锁等待；提前退出的写端直接报告失败。"""
            if writer.poll() is not None:
                raise RuntimeError("数据驱动在锁等待前退出: " + writer.stdout.read().decode())
            rows = sql("SELECT COUNT(DISTINCT w.REQUESTING_ENGINE_TRANSACTION_ID) "
                       "FROM performance_schema.data_lock_waits w "
                       "JOIN performance_schema.data_locks l "
                       "ON w.REQUESTING_ENGINE_LOCK_ID=l.ENGINE_LOCK_ID "
                       "WHERE l.OBJECT_SCHEMA='sample' AND l.OBJECT_NAME='sample_counter';")
            return int(rows.stdout.strip()) >= 2

        retry("数据任务进入 InnoDB 行锁等待", writer_waits, timeout=15)
        writer.stdin.write(b"locked\n")
        writer.stdin.flush()
        lines = wait_line(writer, b"SCENE_ADVANCED")
        lock.stdin.write(b"COMMIT;\n")
        lock.stdin.close()
        if lock.wait(timeout=5) != 0:
            raise RuntimeError("释放隔离锁事务失败")
        output, _ = writer.communicate(timeout=25)
        if writer.returncode:
            raise RuntimeError("样例写入失败: " + output.decode())
        for line in lines:
            if line.startswith("space="):
                print(line, flush=True)
        print("write: InnoDB 行锁等待期间 Scene 已推进", flush=True)

    drive("offline")
    call(["docker", "start", broker])
    retry("RabbitMQ 重启并监听 AMQP", broker_ready, timeout=60)
    # Docker 在其他节点尚在运行时也可能为重启的容器分配新地址。
    driver_env["POP_AMQP_URI"] = f"amqp://sample:{mq_secret}@{address(broker)}:5672/"
    drive("deliver")
    for node in nodes:
        redis(node, "FLUSHDB")
    drive("rebuild")


def main():
    """生成本次独享的资源和凭据；失败时保留错误上下文并遮蔽凭据。"""
    passwords = [secrets.token_hex(20) for _ in range(4)]

    def interrupted(signum, frame):
        """让 SIGTERM 经资源清理路径退出。"""
        raise RuntimeError("隔离验收被终止")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            run_fixture(str(Path(sys.argv[1]).resolve()), Path(directory), stack, passwords)
    except Exception as error:
        detail = str(error)
        for password in passwords:
            detail = detail.replace(password, "<redacted>")
        print(detail, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
