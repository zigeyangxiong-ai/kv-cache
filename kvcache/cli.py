"""命令行客户端，演示时手动敲命令用。

  python -m kvcache.cli set user:1 hello --ttl 60
  python -m kvcache.cli get user:1
  python -m kvcache.cli query demo:hot:1      查缓存，没命中就回源（演示缓存击穿防护）
  python -m kvcache.cli route user:1
  python -m kvcache.cli stats
  python -m kvcache.cli metrics               回源次数等计数
"""

import argparse
import json
from urllib.parse import quote

from .httpjson import call


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gateway", default="http://127.0.0.1:8000")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("set")
    s.add_argument("key")
    s.add_argument("value")
    s.add_argument("--ttl", type=float, default=0)
    g = sub.add_parser("get")
    g.add_argument("key")
    q = sub.add_parser("query")
    q.add_argument("key")
    r = sub.add_parser("route")
    r.add_argument("key")
    sub.add_parser("stats")
    sub.add_parser("metrics")
    args = ap.parse_args()

    gw = args.gateway
    try:
        if args.cmd == "set":
            res = call("POST", gw + "/set", {"key": args.key, "value": args.value, "ttl_seconds": args.ttl}, timeout=3)
        elif args.cmd == "get":
            res = call("GET", gw + "/get?key=" + quote(args.key, safe=""), timeout=3)
        elif args.cmd == "query":
            res = call("GET", gw + "/query?key=" + quote(args.key, safe=""), timeout=8)
        elif args.cmd == "route":
            res = call("GET", gw + "/route?key=" + quote(args.key, safe=""), timeout=3)
        elif args.cmd == "metrics":
            res = call("GET", gw + "/debug/metrics", timeout=3)
        else:
            res = call("GET", gw + "/stats", timeout=5)
    except OSError as e:
        res = {"ok": False, "error": "GATEWAY_UNREACHABLE", "detail": str(e)}
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
