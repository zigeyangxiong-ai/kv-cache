"""Store 的单元测试，不需要启动集群：python -m unittest discover -s tests -v"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvcache.gateway import VersionCounter, stable_hash  # noqa: E402
from kvcache.store import Store  # noqa: E402


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class StoreTest(unittest.TestCase):
    def test_set_then_get(self):
        s = Store()
        s.set("a", "1", 1, 0)
        self.assertEqual(s.get("a"), ("1", 1))
        self.assertIsNone(s.get("missing"))

    def test_newer_version_overwrites(self):
        s = Store()
        s.set("a", "old", 1, 0)
        self.assertEqual(s.set("a", "new", 2, 0), (2, True))
        self.assertEqual(s.get("a"), ("new", 2))

    def test_older_version_is_ignored(self):
        s = Store()
        s.set("a", "new", 2, 0)
        self.assertEqual(s.set("a", "old", 1, 0), (2, False))
        self.assertEqual(s.get("a"), ("new", 2))

    def test_same_version_duplicate_is_ignored(self):
        clock = FakeClock()
        s = Store(clock=clock)
        s.set("a", "v", 5, 1060)
        for _ in range(2):
            self.assertEqual(s.set("a", "v", 5, 9999), (5, False))
        clock.t = 1061
        self.assertIsNone(s.get("a"), "重复消息不应该延长过期时间")

    def test_without_version_check_old_overwrites_new(self):
        s = Store(check_version=False)
        s.set("a", "new", 2, 0)
        s.set("a", "old", 1, 0)
        self.assertEqual(s.get("a"), ("old", 1))

    def test_ttl_expires(self):
        clock = FakeClock()
        s = Store(clock=clock)
        s.set("a", "v", 1, 1010)
        clock.t = 1009.9
        self.assertEqual(s.get("a"), ("v", 1))
        clock.t = 1010
        self.assertIsNone(s.get("a"))
        self.assertEqual(s.stats()["expired"], 1)

    def test_zero_ttl_never_expires(self):
        clock = FakeClock()
        s = Store(clock=clock)
        s.set("a", "v", 1, 0)
        clock.t = 10**9
        self.assertEqual(s.get("a"), ("v", 1))

    def test_expired_key_accepts_lower_version(self):
        clock = FakeClock()
        s = Store(clock=clock)
        s.set("a", "v", 5, 1010)
        clock.t = 1020
        self.assertEqual(s.set("a", "w", 3, 0), (3, True))

    def test_lru_evicts_least_recently_used(self):
        s = Store(capacity=3)
        s.set("A", "a", 1, 0)
        s.set("B", "b", 2, 0)
        s.set("C", "c", 3, 0)
        s.get("A")  # A 变成最近用过，B 成为最久没用的
        s.set("D", "d", 4, 0)
        self.assertEqual(sorted(s.keys()), ["A", "C", "D"])
        self.assertIsNone(s.get("B"))
        self.assertEqual(s.stats()["evicted"], 1)

    def test_lru_update_counts_as_use(self):
        s = Store(capacity=2)
        s.set("A", "a", 1, 0)
        s.set("B", "b", 2, 0)
        s.set("A", "a2", 3, 0)
        s.set("C", "c", 4, 0)
        self.assertEqual(sorted(s.keys()), ["A", "C"])

    def test_clock_going_back_revives_expired_entry(self):
        # 这条测试记录的是一个已知限制：TTL 依赖物理时钟
        clock = FakeClock()
        s = Store(clock=clock)
        s.set("a", "v", 1, 1010)
        clock.t = 1015
        clock.t -= 60
        self.assertEqual(s.get("a"), ("v", 1))


class GatewayHelpersTest(unittest.TestCase):
    def test_stable_hash_is_stable(self):
        self.assertEqual(stable_hash("user:1"), stable_hash("user:1"))
        self.assertEqual(stable_hash("demo") % 3, 0)

    def test_version_counter_survives_restart(self):
        import tempfile
        d = tempfile.mkdtemp()
        path = os.path.join(d, "state.json")
        c1 = VersionCounter(path)
        used = [c1.next() for _ in range(5)]
        c2 = VersionCounter(path)
        self.assertGreater(c2.next(), max(used))


if __name__ == "__main__":
    unittest.main()
