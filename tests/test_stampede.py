"""防击穿相关的单元测试：python -m unittest discover -s tests -v"""

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvcache.stampede import MockOrigin, OriginUnavailable, SingleFlight  # noqa: E402


def run_together(n, fn):
    """n 个线程在同一时刻起跑，返回各自的结果或异常。"""
    barrier = threading.Barrier(n)
    out = [None] * n

    def worker(i):
        barrier.wait()
        try:
            out[i] = fn()
        except Exception as e:
            out[i] = e

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(10)
    return out


class SingleFlightTest(unittest.TestCase):
    def test_concurrent_same_key_runs_once(self):
        sf = SingleFlight()
        calls = []

        def slow():
            calls.append(1)
            time.sleep(0.2)
            return "v"

        out = run_together(100, lambda: sf.do("k", slow))
        self.assertEqual(len(calls), 1)
        self.assertTrue(all(r == ("v", True) or r == ("v", False) for r in out))
        self.assertEqual(sum(1 for r in out if r[1] is False), 1, "只有一个请求是真正执行的那个")

    def test_failure_reaches_all_waiters_and_clears(self):
        sf = SingleFlight()

        def boom():
            time.sleep(0.1)
            raise OriginUnavailable("k")

        out = run_together(20, lambda: sf.do("k", boom))
        self.assertTrue(all(isinstance(r, OriginUnavailable) for r in out))
        self.assertEqual(sf.do("k", lambda: "ok"), ("ok", False), "失败后状态被清理，下一次能正常执行")

    def test_different_keys_do_not_block_each_other(self):
        sf = SingleFlight()
        start = time.time()
        out = run_together(5, lambda: sf.do(threading.current_thread().name, lambda: time.sleep(0.3) or "v"))
        self.assertTrue(all(r == ("v", False) for r in out))
        self.assertLess(time.time() - start, 1.0, "5 个不同 key 应该并发执行，而不是排队 1.5 秒")

    def test_waiter_timeout(self):
        sf = SingleFlight()
        t = threading.Thread(target=lambda: sf.do("k", lambda: time.sleep(0.5)))
        t.start()
        time.sleep(0.05)
        with self.assertRaises(TimeoutError):
            sf.do("k", lambda: "never", timeout=0.1)
        t.join()


class MockOriginTest(unittest.TestCase):
    def test_counts_and_known_keys(self):
        o = MockOrigin(delay=0)
        self.assertEqual(o.load("demo:hot:1"), "demo-result-1")
        self.assertIsNone(o.load("user:1"), "非 demo 的 key 不当作后端数据")
        self.assertEqual(o.load_count, 2)


if __name__ == "__main__":
    unittest.main()
