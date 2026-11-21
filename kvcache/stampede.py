"""防缓存击穿：模拟数据源 + 按 key 合并并发回源（Singleflight）。

都不涉及网络，入口（gateway.py）调用它们，单元测试也可以直接测。
"""

import threading
import time


class OriginUnavailable(Exception):
    pass


class MockOrigin:
    """模拟一个慢的后端数据库。只认 demo: 开头的预置 key。

    只处理演示用的 key，是为了不把用户 /set 进来的数据当成“后端真实数据”：
    否则缓存被淘汰后再回源，拿到的会是和用户最后写入不一致的内容。
    """

    def __init__(self, delay=0.15, fail_keys=()):
        self.delay = delay
        self.fail_keys = set(fail_keys)
        self.load_count = 0
        self._lock = threading.Lock()
        self.data = {f"demo:hot:{i}": f"demo-result-{i}" for i in range(1, 11)}

    def load(self, key):
        with self._lock:
            self.load_count += 1
        time.sleep(self.delay)
        if key in self.fail_keys:
            raise OriginUnavailable(key)
        return self.data.get(key)  # None 表示后端也没有


class _Call:
    def __init__(self):
        self.done = threading.Event()
        self.result = None
        self.error = None
        self.waiters = 0


class SingleFlight:
    """同一个 key 同一时刻只让一个调用执行 fn，其余调用等着拿同一个结果。

    不同 key 之间互不影响，可以同时回源。只在本进程内有效。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._calls = {}

    def do(self, key, fn, timeout=5.0):
        """返回 (result, shared)。shared=True 表示这次是等别人的结果，没有自己执行 fn。"""
        with self._lock:
            c = self._calls.get(key)
            if c is not None:
                c.waiters += 1
                leader = False
            else:
                c = _Call()
                self._calls[key] = c
                leader = True

        if not leader:
            if not c.done.wait(timeout):
                raise TimeoutError(f"等待 {key} 的回源结果超时")
            if c.error is not None:
                raise c.error
            return c.result, True

        try:
            c.result = fn()
        except Exception as e:  # 失败也要通知等待者，不能让它们一直挂着
            c.error = e
        finally:
            with self._lock:
                self._calls.pop(key, None)
            c.done.set()
        if c.error is not None:
            raise c.error
        return c.result, False
