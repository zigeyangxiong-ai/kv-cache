"""单个节点内的存储：字典 + TTL 过期 + LRU 淘汰 + 版本比较。

这里不涉及网络，方便单独测试。节点进程（node.py）只是把它包装成 HTTP 接口。
"""

import threading
import time
from collections import OrderedDict


class Store:
    def __init__(self, capacity=1000, clock=time.time, check_version=True):
        self.capacity = capacity
        self.check_version = check_version  # 只有“删掉版本比较”的对照实验才设为 False
        self._clock = clock  # 测试时可以换成假时钟
        self._data = OrderedDict()  # key -> (value, version, expire_at)，越靠后越“最近用过”
        self._lock = threading.Lock()
        self.evicted = 0
        self.expired = 0
        self.ignored_old = 0

    def _is_expired(self, expire_at):
        return expire_at != 0 and self._clock() >= expire_at

    def set(self, key, value, version, expire_at):
        """保存一条记录。返回 (stored_version, applied)。

        applied=False 表示本地已有相同或更新的版本，这次写入被忽略。
        """
        with self._lock:
            old = self._data.get(key)
            if old is not None and self._is_expired(old[2]):
                del self._data[key]
                self.expired += 1
                old = None

            if self.check_version and old is not None and old[1] >= version:
                self.ignored_old += 1
                return old[1], False

            self._data[key] = (value, version, expire_at)
            self._data.move_to_end(key)
            while len(self._data) > self.capacity:
                self._data.popitem(last=False)
                self.evicted += 1
            return version, True

    def get(self, key):
        """返回 (value, version)，没有或已过期返回 None。读取也算“用过”。"""
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return None
            value, version, expire_at = item
            if self._is_expired(expire_at):
                del self._data[key]
                self.expired += 1
                return None
            self._data.move_to_end(key)
            return value, version

    def keys(self):
        with self._lock:
            return list(self._data.keys())

    def stats(self):
        with self._lock:
            return {
                "size": len(self._data),
                "capacity": self.capacity,
                "evicted": self.evicted,
                "expired": self.expired,
                "ignored_old": self.ignored_old,
            }
