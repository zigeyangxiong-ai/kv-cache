"""在本机启动 3 个节点 + 1 个入口（各自是独立进程），并能单独停止/重启某个节点。

命令行：python -m kvcache.cluster          启动整个集群，Ctrl+C 结束
代码里：实验脚本用 Cluster 类来停节点、重启节点。
"""

import os
import subprocess
import sys
import time

from .httpjson import call

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GATEWAY_PORT = 8000
NODE_PORTS = [8001, 8002, 8003]


class Cluster:
    def __init__(self, gateway_port=GATEWAY_PORT, node_ports=NODE_PORTS, capacity=1000,
                 write_quorum=1, version_check=True, state_file=None, log_dir=None):
        self.gateway_port = gateway_port
        self.node_ports = list(node_ports)
        self.capacity = capacity
        self.write_quorum = write_quorum
        self.version_check = version_check
        self.state_file = state_file or os.path.join(ROOT, "gateway_state.json")
        self.log_dir = log_dir or os.path.join(ROOT, "logs")
        os.makedirs(self.log_dir, exist_ok=True)
        self.nodes = [None] * len(self.node_ports)
        self.gateway = None
        self._logs = []

    @property
    def gateway_url(self):
        return f"http://127.0.0.1:{self.gateway_port}"

    def node_url(self, i):
        return f"http://127.0.0.1:{self.node_ports[i]}"

    def _spawn(self, args, log_name):
        log = open(os.path.join(self.log_dir, log_name), "a", encoding="utf-8")
        self._logs.append(log)
        env = dict(os.environ, PYTHONIOENCODING="utf-8")
        return subprocess.Popen([sys.executable, "-m", *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env=env)

    def start_node(self, i):
        args = ["kvcache.node", "--id", str(i), "--port", str(self.node_ports[i]), "--capacity", str(self.capacity)]
        if not self.version_check:
            args.append("--unsafe-no-version-check")
        self.nodes[i] = self._spawn(args, f"node{i}.log")
        _wait_up(self.node_url(i) + "/internal/stats")

    def stop_node(self, i):
        p = self.nodes[i]
        if p is not None and p.poll() is None:
            p.kill()
            p.wait()
        self.nodes[i] = None

    def restart_node(self, i):
        self.stop_node(i)
        self.start_node(i)

    def start(self):
        for i in range(len(self.node_ports)):
            self.start_node(i)
        nodes = ",".join(f"127.0.0.1:{p}" for p in self.node_ports)
        self.gateway = self._spawn(["kvcache.gateway", "--port", str(self.gateway_port), "--nodes", nodes,
                                    "--write-quorum", str(self.write_quorum), "--state-file", self.state_file],
                                   "gateway.log")
        _wait_up(self.gateway_url + "/stats")
        return self

    def stop(self):
        procs = [self.gateway] + self.nodes
        for p in procs:
            if p is not None and p.poll() is None:
                p.kill()
                p.wait()
        for f in self._logs:
            f.close()
        self._logs = []

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


def _wait_up(url, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            call("GET", url, timeout=0.5)
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"服务没有按时启动：{url}（看 logs/ 目录里的日志）")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--capacity", type=int, default=1000)
    ap.add_argument("--write-quorum", type=int, default=1)
    args = ap.parse_args()

    c = Cluster(capacity=args.capacity, write_quorum=args.write_quorum)
    c.start()
    print(f"集群已启动：入口 {c.gateway_url}，节点端口 {c.node_ports}")
    print("另开一个窗口，用 python -m kvcache.cli 操作。按 Ctrl+C 结束。")
    try:
        while True:
            time.sleep(1)
            for i, p in enumerate(c.nodes):
                if p is not None and p.poll() is not None:
                    print(f"节点 {i} 已退出")
                    c.nodes[i] = None
    except KeyboardInterrupt:
        pass
    finally:
        c.stop()
        print("集群已停止")


if __name__ == "__main__":
    main()
