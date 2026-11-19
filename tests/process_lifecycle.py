"""以子进程验证四类服务的就绪与退出、空 Space 的推进，以及配置错误的诊断。"""

import json
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


def read_events(role, pipe, events):
    """把子进程日志逐行连同角色放入事件队列，以 None 标记输出结束。"""
    for line in pipe:
        events.put((role, line.rstrip()))
    events.put((role, None))


def check_lifecycle(commands, expect_ticks):
    """等待指定进程就绪，按放置情况观察空场 Tick，并验证信号停机。"""
    processes = {}
    events = queue.Queue()
    ready = set()
    first_ticks = set()
    advanced = set()
    try:
        for role, args in commands.items():
            process = subprocess.Popen(args, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True)
            processes[role] = process
            threading.Thread(target=read_events, args=(role, process.stdout, events),
                             daemon=True).start()

        deadline = time.monotonic() + 10
        while len(ready) < len(commands) or (expect_ticks and (not advanced or first_ticks != advanced)):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(f"timeout: ready={ready}, first_ticks={first_ticks}, advanced={advanced}")
            role, line = events.get(timeout=remaining)
            if line is None:
                raise RuntimeError(f"{role} exited before shutdown")
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            event = fields.get("event")
            if event in {"tick", "ready"}:
                print(line)
            if fields.get("role") != role:
                raise RuntimeError(f"incorrect process context: {line}")
            if event == "ready":
                instance = "worker_instance_id" if role == "game_worker" else "instance_id"
                if not fields.get(instance):
                    raise RuntimeError(f"missing instance identity: {line}")
                ready.add(role)
            elif event == "tick":
                if not expect_ticks:
                    raise RuntimeError(f"unassigned Worker advanced a Space: {line}")
                if any(int(fields[key]) for key in ("entities", "inputs", "dispatched", "commits")):
                    raise RuntimeError(f"idle Space produced work: {line}")
                first_ticks.add(fields["space"])
                if int(fields["tick"]) > 1:
                    advanced.add(fields["space"])
        for index, process in enumerate(processes.values()):
            process.send_signal(signal.SIGINT if index % 2 else signal.SIGTERM)
        for role, process in processes.items():
            code = process.wait(timeout=5)
            if code != 0:
                raise RuntimeError(f"{role} returned {code}")
        stopped = set()
        while len(stopped) < len(commands):
            role, line = events.get(timeout=3)
            if line and not expect_ticks and "event=space_stop " in line:
                raise RuntimeError(f"unassigned Worker created a Space: {line}")
            if line and "event=stopped " in line:
                print(line)
                stopped.add(role)
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.kill()
            process.wait()


def run_processes(binaries, configs):
    """启动交付配置中的四个角色，检查空场运行与有序退出。"""
    roles = ("gateway", "game_worker", "platform", "control")
    commands = {role: [binaries / role, "--config", configs / f"{role}.yaml"] for role in roles}
    check_lifecycle(commands, expect_ticks=True)


def worker_config(spaces):
    """生成只有指定放置项的独立配置，供空载启动与错误诊断用例使用。"""
    budget = {"max_entries": 4, "max_bytes": 4096, "max_age_ms": 100, "per_source": 1}
    queues = dict.fromkeys(("control", "input", "result", "background"), budget)
    return {"role": "game_worker", "log_entries": 32, "player_shards": 1,
            "scene_shards": 1, "compute_threads": 1, "queues": queues, "spaces": spaces}


def unassigned_worker(binaries, _configs):
    """通过配置入口启动没有分配任何 Space 的 Worker，核对就绪和停机。"""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "standby.yaml"
        path.write_text(json.dumps(worker_config([])))
        check_lifecycle({"game_worker": [binaries / "game_worker", "--config", path]},
                        expect_ticks=False)


def invalid_configs(binaries, _configs):
    """用独立配置验证启动失败时的退出码、诊断和就绪状态。"""
    space = {"space_id": 66, "scene_shard": 0}
    base = worker_config([space])
    cases = [
        ({**base, "role": "gateway"}, "role"),
        ({**base, "queues": {**base["queues"], "input": {}}}, "queues.input"),
        ({**base, "spaces": [space, space]}, "duplicate space_id"),
        ({**base, "spaces": {}}, "spaces must be a sequence"),
    ]
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "invalid.yaml"
        for data, expected in cases:
            path.write_text(json.dumps(data))  # JSON 也是合法的 YAML 输入。
            result = subprocess.run([binaries / "game_worker", "--config", path],
                                    capture_output=True, text=True, timeout=3)
            if result.returncode <= 0 or expected not in result.stderr or "event=ready" in result.stdout:
                raise RuntimeError(f"expected rejection of {expected}: {result}")
            print(result.stderr.strip())


if __name__ == "__main__":
    scenario, binary_dir, config_dir = sys.argv[1:]
    test = {"ProcessLifecycle": run_processes, "UnassignedWorker": unassigned_worker,
            "InvalidConfig": invalid_configs}[scenario]
    test(Path(binary_dir), Path(config_dir))
