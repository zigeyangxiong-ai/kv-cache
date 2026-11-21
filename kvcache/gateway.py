"""入口（路由）：客户端只和它打交道。

  POST /set   {"key", "value", "ttl_seconds"}  ->  {"ok", "version", "acks"}
  GET  /get?key=                              ->  只查缓存：{"ok", "found", "value", "version", "from_node"}
  GET  /query?key=                            ->  查缓存，没命中就回源并写回：{"ok", "found", "value", "source"}
  GET  /route?key=                            ->  这个 key 会落到哪两个节点
  GET  /stats
  GET  /debug/metrics                         ->  回源次数、命中次数等计数（实验用）
  POST /debug/reset                           ->  计数清零（实验用）

启动：python -m kvcache.gateway --port 8000 --nodes 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
"""

import argparse
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .httpjson import JsonHandler, call, serve
from .stampede import MockOrigin, OriginUnavailable, SingleFlight

TIMEOUT = 1.0
REFILL_TTL = 3.0  # 回源拿到的数据写回缓存时用的 TTL
ORIGIN_WAIT = 5.0  # 等待者最多等多久


def stable_hash(key):
    # 不能用 Python 自带的 hash()：它每次启动进程都会变，入口一重启路由就全乱了
    return int.from_bytes(hashlib.md5(key.encode("utf-8")).digest()[:8], "big")


class VersionCounter:
    """入口统一分配版本号。

    把“已经分配到哪里”定期写进文件，入口重启后从文件读回并跳过一段，
    避免新写入拿到比节点上已有记录还小的版本号而被当成旧消息丢掉。
    """

    STEP = 1000

    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        start = 0
        if path and os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                start = int(json.load(f).get("reserved", 0))
        self._next = start + 1
        self._reserved = start
        self._reserve()

    def _reserve(self):
        self._reserved = self._next + self.STEP
        if self.path:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"reserved": self._reserved}, f)
            os.replace(tmp, self.path)

    def next(self):
        with self._lock:
            if self._next > self._reserved:
                self._reserve()
            v = self._next
            self._next += 1
            return v


class Gateway:
    def __init__(self, nodes, write_quorum=1, state_file=None, singleflight=True, origin_delay=0.15):
        self.nodes = nodes
        self.write_quorum = write_quorum
        self.versions = VersionCounter(state_file)
        self.pool = ThreadPoolExecutor(max_workers=32)
        self.singleflight_on = singleflight
        self.origin = MockOrigin(delay=origin_delay)
        self.flight = SingleFlight()
        self._m_lock = threading.Lock()
        self.metrics = {}
        self.reset_metrics()

    def reset_metrics(self):
        with self._m_lock:
            self.metrics = {"query_count": 0, "cache_hit_count": 0, "cache_miss_count": 0,
                            "coalesced_waiter_count": 0, "refill_failed_count": 0}
        self.origin.load_count = 0

    def _bump(self, name):
        with self._m_lock:
            self.metrics[name] += 1

    def replicas(self, key):
        x = stable_hash(key) % len(self.nodes)
        return [x, (x + 1) % len(self.nodes)]

    def _node_set(self, idx, record):
        try:
            r = call("PUT", f"http://{self.nodes[idx]}/internal/set", record, timeout=TIMEOUT)
        except OSError as e:
            return idx, False, f"unreachable: {e.__class__.__name__}"
        # ok=true 只表示节点处理完了；版本号对得上才说明这次的值真的存进去了
        stored = r.get("ok") and r.get("stored_version") == record["version"]
        return idx, bool(stored), r

    def set(self, body):
        if not isinstance(body, dict):
            return 400, {"ok": False, "error": "BAD_REQUEST"}
        key = body.get("key")
        if not key:
            return 400, {"ok": False, "error": "KEY_EMPTY"}
        try:
            ttl = float(body.get("ttl_seconds", 0) or 0)
        except (TypeError, ValueError):
            return 400, {"ok": False, "error": "BAD_REQUEST"}

        version = self.versions.next()
        record = {
            "key": key,
            "value": str(body.get("value", "")),
            "version": version,
            "expire_at": time.time() + ttl if ttl > 0 else 0,
        }
        replicas = self.replicas(key)
        futures = [self.pool.submit(self._node_set, i, record) for i in replicas]

        acks, detail = [], {}
        for f in futures:
            idx, ok, info = f.result()
            detail[idx] = "stored" if ok else info
            if ok:
                acks.append(idx)
        if len(acks) >= self.write_quorum:
            return 200, {"ok": True, "version": version, "acks": acks, "replicas": replicas}
        return 503, {"ok": False, "error": "WRITE_FAILED", "version": version, "replicas": replicas, "detail": detail}

    def get(self, key):
        if not key:
            return 400, {"ok": False, "error": "KEY_EMPTY"}
        replicas = self.replicas(key)
        failed = []
        for idx in replicas:
            try:
                r = call("GET", f"http://{self.nodes[idx]}/internal/get?key={_q(key)}", timeout=TIMEOUT)
            except OSError:
                failed.append(idx)
                continue
            if r.get("ok") and r.get("found"):
                return 200, {"ok": True, "found": True, "value": r["value"], "version": r["version"], "from_node": idx}
        if failed:
            # 有节点没回答，就不能断定“不存在”
            return 503, {"ok": False, "error": "NODES_UNAVAILABLE", "unreachable": failed, "replicas": replicas}
        return 200, {"ok": True, "found": False, "replicas": replicas}

    def _load_and_refill(self, key, recheck):
        """回源并写回缓存。返回 (status, payload)。"""
        if recheck:
            # 拿到回源权以后再看一眼缓存：可能前一批请求刚好写回了
            status, r = self.get(key)
            if status == 200 and r.get("found"):
                return 200, {"ok": True, "found": True, "value": r["value"], "source": "cache"}
        try:
            value = self.origin.load(key)
        except OriginUnavailable:
            return 503, {"ok": False, "error": "ORIGIN_UNAVAILABLE"}
        if value is None:
            # 后端也没有。这一版没做负缓存，每次查不存在的 key 都会回源一次
            return 200, {"ok": True, "found": False, "source": "origin"}
        status, w = self.set({"key": key, "value": value, "ttl_seconds": REFILL_TTL})
        payload = {"ok": True, "found": True, "value": value, "source": "origin", "cache_filled": status == 200}
        if status != 200:
            self._bump("refill_failed_count")
        return 200, payload

    def query(self, key):
        if not key:
            return 400, {"ok": False, "error": "KEY_EMPTY"}
        self._bump("query_count")
        status, r = self.get(key)
        if status == 200 and r.get("found"):
            self._bump("cache_hit_count")
            return 200, {"ok": True, "found": True, "value": r["value"], "source": "cache"}
        if status != 200:
            # 两个副本都连不上：不能当成普通的缓存未命中去回源
            return status, r
        self._bump("cache_miss_count")

        if not self.singleflight_on:
            return self._load_and_refill(key, recheck=False)
        try:
            (status, payload), shared = self.flight.do(
                key, lambda: self._load_and_refill(key, recheck=True), timeout=ORIGIN_WAIT)
        except TimeoutError:
            return 504, {"ok": False, "error": "ORIGIN_TIMEOUT"}
        if shared:
            self._bump("coalesced_waiter_count")
            payload = dict(payload, shared=True)
        return status, payload

    def debug_metrics(self):
        with self._m_lock:
            m = dict(self.metrics)
        m["origin_load_count"] = self.origin.load_count
        m["singleflight"] = self.singleflight_on
        m["origin_delay_ms"] = int(self.origin.delay * 1000)
        return 200, {"ok": True, **m}

    def stats(self):
        out = []
        for i, addr in enumerate(self.nodes):
            try:
                out.append(call("GET", f"http://{addr}/internal/stats", timeout=TIMEOUT))
            except OSError:
                out.append({"ok": False, "node": i, "error": "unreachable"})
        return 200, {"ok": True, "write_quorum": self.write_quorum, "nodes": out}

    def route(self, method, path, query, body):
        if method == "POST" and path == "/set":
            return self.set(body)
        if method == "GET" and path == "/get":
            return self.get(query.get("key", ""))
        if method == "GET" and path == "/route":
            key = query.get("key", "")
            if not key:
                return 400, {"ok": False, "error": "KEY_EMPTY"}
            x, y = self.replicas(key)
            return 200, {"ok": True, "key": key, "hash_mod_3": x, "primary": x, "backup": y}
        if method == "GET" and path == "/query":
            return self.query(query.get("key", ""))
        if method == "GET" and path == "/stats":
            return self.stats()
        if method == "GET" and path == "/debug/metrics":
            return self.debug_metrics()
        if method == "POST" and path == "/debug/reset":
            self.reset_metrics()
            return 200, {"ok": True}
        return 404, {"ok": False, "error": "NOT_FOUND"}


def _q(s):
    from urllib.parse import quote
    return quote(s, safe="")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--nodes", required=True, help="逗号分隔，例如 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003")
    ap.add_argument("--write-quorum", type=int, default=1)
    ap.add_argument("--state-file", default="gateway_state.json")
    ap.add_argument("--origin-delay", type=float, default=0.15, help="模拟数据源每次查询耗时（秒）")
    args = ap.parse_args()

    # 对照实验用：ENABLE_SINGLEFLIGHT=false 时关闭请求合并
    sf = os.environ.get("ENABLE_SINGLEFLIGHT", "true").lower() not in ("0", "false", "no", "off")
    gw = Gateway(args.nodes.split(","), write_quorum=args.write_quorum, state_file=args.state_file,
                 singleflight=sf, origin_delay=args.origin_delay)

    class Handler(JsonHandler):
        def route(self, method, path, query, body):
            return gw.route(method, path, query, body)

    server = serve(Handler, args.port)
    print(f"[gateway] listening on 127.0.0.1:{args.port} nodes={args.nodes} W={args.write_quorum} "
          f"singleflight={'on' if sf else 'off'}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
