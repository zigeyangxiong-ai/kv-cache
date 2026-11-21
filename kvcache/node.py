"""存储节点：把 Store 包装成 HTTP 接口。

接口（只有入口和实验脚本会调用）：
  PUT  /internal/set      {"key", "value", "version", "expire_at"}
  GET  /internal/get?key=
  GET  /internal/stats
  POST /admin/clock       {"offset": 秒}   调整本节点时钟，用于“时间倒退”实验

启动：python -m kvcache.node --id 0 --port 8001
"""

import argparse
import threading
import time

from .httpjson import JsonHandler, serve
from .store import Store


class Node:
    def __init__(self, node_id, capacity, version_check=True):
        self.node_id = node_id
        self.clock_offset = 0.0
        self.store = Store(capacity=capacity, clock=self.now, check_version=version_check)
        self.version_check = version_check
        self.counts = {"set": 0, "get": 0}
        self._count_lock = threading.Lock()

    def now(self):
        return time.time() + self.clock_offset

    def _count(self, name):
        with self._count_lock:
            self.counts[name] += 1

    def route(self, method, path, query, body):
        if method == "PUT" and path == "/internal/set":
            self._count("set")
            if not isinstance(body, dict) or not body.get("key") or "version" not in body:
                return 400, {"ok": False, "error": "BAD_REQUEST"}
            version = int(body["version"])
            stored, applied = self.store.set(body["key"], body.get("value", ""), version, float(body.get("expire_at", 0)))
            return 200, {"ok": True, "stored_version": stored, "applied": applied}

        if method == "GET" and path == "/internal/get":
            self._count("get")
            key = query.get("key", "")
            if not key:
                return 400, {"ok": False, "error": "KEY_EMPTY"}
            item = self.store.get(key)
            if item is None:
                return 200, {"ok": True, "found": False}
            value, version = item
            return 200, {"ok": True, "found": True, "value": value, "version": version}

        if method == "GET" and path == "/internal/stats":
            with self._count_lock:
                counts = dict(self.counts)
            return 200, {"ok": True, "node": self.node_id, "requests": counts,
                         "clock_offset": self.clock_offset, **self.store.stats()}

        if method == "POST" and path == "/admin/clock":
            self.clock_offset = float((body or {}).get("offset", 0))
            return 200, {"ok": True, "clock_offset": self.clock_offset}

        return 404, {"ok": False, "error": "NOT_FOUND"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--capacity", type=int, default=1000)
    ap.add_argument("--unsafe-no-version-check", action="store_true")
    args = ap.parse_args()

    node = Node(args.id, args.capacity, version_check=not args.unsafe_no_version_check)

    class Handler(JsonHandler):
        def route(self, method, path, query, body):
            return node.route(method, path, query, body)

    server = serve(Handler, args.port)
    print(f"[node {args.id}] listening on 127.0.0.1:{args.port} capacity={args.capacity}"
          + (" (version check OFF)" if not node.version_check else ""), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
