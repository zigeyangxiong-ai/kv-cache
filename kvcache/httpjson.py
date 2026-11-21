"""节点和入口共用的 HTTP/JSON 小工具，只用 Python 标准库。"""

import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class JsonHandler(BaseHTTPRequestHandler):
    """子类实现 route(method, path, query, body) -> (status, dict)。"""

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle(self, method):
        from urllib.parse import parse_qs, urlparse

        url = urlparse(self.path)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        body = None
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return self._send(400, {"ok": False, "error": "BAD_REQUEST"})
        try:
            status, payload = self.route(method, url.path, query, body)
        except Exception as e:  # 不让一个坏请求把整个进程弄崩
            status, payload = 500, {"ok": False, "error": "INTERNAL_ERROR", "detail": str(e)}
        self._send(status, payload)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_PUT(self):
        self._handle("PUT")


def serve(handler_cls, port):
    server = ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    server.daemon_threads = True
    return server


def call(method, url, body=None, timeout=1.0):
    """发一个 JSON 请求。网络失败时抛出 OSError（含超时、连接被拒）。"""
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode("utf-8"))
