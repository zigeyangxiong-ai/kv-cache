"""故障注入实验：每个实验先写预测，再操作，再对照实际结果。

  python experiments/run_experiments.py            跑全部实验
  python experiments/run_experiments.py E1 E3      只跑指定实验

结果保存在 experiments/results/ 下（Markdown 记录 + E5 的 CSV 和图）。
实验用 18000 段端口，不影响你手动开的 8000 集群。
"""

import csv
import json
import os
import random
import sys
import time
from datetime import datetime
from urllib.parse import quote

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from kvcache.cluster import Cluster  # noqa: E402
from kvcache.gateway import stable_hash  # noqa: E402
from kvcache.httpjson import call  # noqa: E402

RESULTS = os.path.join(HERE, "results")
PORTS = dict(gateway_port=18000, node_ports=[18001, 18002, 18003])


def new_cluster(**kw):
    state = os.path.join(RESULTS, "_gateway_state.json")
    if os.path.exists(state):
        os.remove(state)
    return Cluster(state_file=state, log_dir=os.path.join(RESULTS, "logs"), **PORTS, **kw)


def gw_set(c, key, value, ttl=0):
    return call("POST", c.gateway_url + "/set", {"key": key, "value": value, "ttl_seconds": ttl}, timeout=3)


def gw_get(c, key):
    return call("GET", c.gateway_url + "/get?key=" + quote(key, safe=""), timeout=3)


def node_set(c, i, record):
    return call("PUT", c.node_url(i) + "/internal/set", record, timeout=3)


def node_get(c, i, key):
    return call("GET", c.node_url(i) + "/internal/get?key=" + quote(key, safe=""), timeout=3)


def key_on(primary, prefix="k"):
    """找一个 hash % 3 正好等于 primary 的 key，方便实验里指定节点。"""
    for n in range(10000):
        k = f"{prefix}{n}"
        if stable_hash(k) % 3 == primary:
            return k
    raise RuntimeError("找不到合适的 key")


class Record:
    def __init__(self, eid, title, guarantee):
        self.eid, self.title, self.guarantee = eid, title, guarantee
        self.steps, self.prediction, self.observed, self.explain = [], "", [], ""
        self.passed = None

    def predict(self, text):
        self.prediction = text
        print(f"\n===== {self.eid} {self.title} =====")
        print(f"[预测] {text}")

    def step(self, text, result=None):
        line = text if result is None else f"{text} -> {json.dumps(result, ensure_ascii=False)}"
        self.steps.append(line)
        print(f"  - {line}")

    def see(self, text):
        self.observed.append(text)
        print(f"[实际] {text}")

    def done(self, passed, explain):
        self.passed, self.explain = passed, explain
        print(f"[对照] {'符合预测' if passed else '与预测不符'}：{explain}")

    def markdown(self):
        out = [f"## {self.eid} {self.title}", "",
               f"**相关保证：** {self.guarantee}", "",
               f"**实验前预测：** {self.prediction}", "", "**操作与返回：**", ""]
        out += [f"{i}. `{s}`" if "->" in s else f"{i}. {s}" for i, s in enumerate(self.steps, 1)]
        out += ["", "**实际结果：**", ""] + [f"- {o}" for o in self.observed]
        out += ["", f"**对照：** {'符合预测' if self.passed else '与预测不符'}", "",
                f"**解释：** {self.explain}", ""]
        return "\n".join(out)


# ---------------------------------------------------------------- E1
def e1():
    r = Record("E1", "一个副本突然宕机", "写入只要 1 个副本确认（W=1）；另一个副本还在时仍能读到")
    r.predict("先确认 key 在两个副本上都存好，再停掉首选节点。入口读取会先问首选节点失败，转去问备份节点，仍然读到原值；"
              "之后的新写入只有备份节点能确认，W=1 所以仍然返回成功。")
    with new_cluster() as c:
        k = key_on(0, "e1-")
        res = gw_set(c, k, "v1", 0)
        r.step(f"写入 {k}=v1（首选节点 0，备份节点 1）", res)
        a, b = node_get(c, 0, k), node_get(c, 1, k)
        r.step("直接查节点 0", a)
        r.step("直接查节点 1", b)
        both = a.get("found") and b.get("found")
        c.stop_node(0)
        r.step("停掉节点 0（kill 进程）")
        g = gw_get(c, k)
        r.step(f"通过入口读 {k}", g)
        w = gw_set(c, k, "v2", 0)
        r.step(f"通过入口写 {k}=v2", w)
        g2 = gw_get(c, k)
        r.step(f"再读 {k}", g2)
        r.see(f"停节点前两个副本都有数据：{bool(both)}")
        r.see(f"停节点后读到 value={g.get('value')}，来自节点 {g.get('from_node')}")
        r.see(f"停节点后写入 ok={w.get('ok')}，确认的节点 acks={w.get('acks')}")
        ok = bool(both) and g.get("value") == "v1" and g.get("from_node") == 1 and w.get("ok") and w.get("acks") == [1] and g2.get("value") == "v2"
        r.done(ok, "读取在首选节点连不上时自动换到备份节点；写入只得到 1 个确认也算成功，这正是 W=1 的含义。"
                   "代价是：此刻这条新数据只有 1 份，如果节点 1 也挂掉，v2 就丢了。")
    return r


# ---------------------------------------------------------------- E1b
def e1b():
    r = Record("E1b", "写入成功后唯一存好的节点也宕机", "W=1 不保证两份都存好")
    r.predict("先停掉备份节点，再写入：只有首选节点确认，入口仍返回成功。然后停掉首选节点，再启动备份节点。"
              "备份节点上没有这条数据，首选节点又连不上，入口应返回 NODES_UNAVAILABLE，而不是“未找到”。")
    with new_cluster() as c:
        k = key_on(0, "e1b-")
        c.stop_node(1)
        r.step("先停掉备份节点 1")
        w = gw_set(c, k, "only-one-copy", 0)
        r.step(f"写入 {k}", w)
        c.stop_node(0)
        r.step("再停掉首选节点 0")
        c.start_node(1)
        r.step("重新启动节点 1（内存是空的）")
        g = gw_get(c, k)
        r.step(f"通过入口读 {k}", g)
        r.see(f"写入时 ok={w.get('ok')}，acks={w.get('acks')}（只有 1 个节点确认）")
        r.see(f"之后读取返回 ok={g.get('ok')}，error={g.get('error')}，连不上的节点 {g.get('unreachable')}")
        ok = w.get("ok") and w.get("acks") == [0] and g.get("error") == "NODES_UNAVAILABLE"
        r.done(ok, "客户端曾经收到“写入成功”，但数据只存在于已宕机的节点 0 上。这就是“写入成功究竟表示几个节点保存了”的答案："
                   "W=1 时只保证至少 1 个。入口没有说“不存在”，因为节点 0 只是暂时连不上，无法判断。")
    return r


# ---------------------------------------------------------------- E2
def e2():
    r = Record("E2", "同一条内部写入消息重复发送 3 次", "版本号相同的重复消息不改变值，也不延长过期时间")
    r.predict("第 1 次被保存（applied=true），第 2、3 次被识别为重复（applied=false），值和到期时间都保持第 1 次的。")
    with new_cluster() as c:
        k = key_on(0, "e2-")
        now = time.time()
        rec = {"key": k, "value": "same", "version": 7, "expire_at": now + 2}
        res = []
        for n in range(3):
            changed = dict(rec, expire_at=now + 2 + 100 * n)  # 模拟重复消息里带了更晚的到期时间
            res.append(node_set(c, 0, changed))
            r.step(f"第 {n + 1} 次发送 version=7 到节点 0", res[-1])
        time.sleep(2.3)
        g = node_get(c, 0, k)
        r.step("等 2.3 秒后查节点 0", g)
        stats = call("GET", c.node_url(0) + "/internal/stats")
        r.see(f"三次 applied 分别为 {[x.get('applied') for x in res]}")
        r.see(f"2.3 秒后 found={g.get('found')}（按第 1 次的 2 秒过期）；节点统计 ignored_old={stats.get('ignored_old')}")
        ok = [x.get("applied") for x in res] == [True, False, False] and not g.get("found")
        r.done(ok, "节点比较版本号，相同版本直接忽略，所以重复消息是幂等的。注意：这里重复的是同一条内部消息；"
                   "如果客户端把 /set 请求本身重发 3 次，入口会分配 3 个不同的版本号，那是 3 次独立写入。")
    return r


# ---------------------------------------------------------------- E3
def e3():
    r = Record("E3", "消息延迟 10 秒后乱序到达", "节点上版本号只增不减，旧消息不会覆盖新值")
    r.predict("先把 version=2 的新值写到节点，再让 version=1 的旧值延迟 10 秒后到达。开启版本比较时，旧值被忽略；"
              "作为对照，关掉版本比较时，旧值会把新值覆盖掉。")
    k = key_on(0, "e3-")
    results = {}
    for check in (True, False):
        with new_cluster(version_check=check) as c:
            tag = "开启版本比较" if check else "关闭版本比较（对照组）"
            a = node_set(c, 0, {"key": k, "value": "NEW", "version": 2, "expire_at": 0})
            r.step(f"[{tag}] 先到达：version=2 value=NEW", a)
            r.step(f"[{tag}] 旧消息在路上延迟 10 秒……")
            time.sleep(10)
            b = node_set(c, 0, {"key": k, "value": "OLD", "version": 1, "expire_at": 0})
            r.step(f"[{tag}] 后到达：version=1 value=OLD", b)
            g = node_get(c, 0, k)
            r.step(f"[{tag}] 查节点 0", g)
            results[check] = g.get("value")
    r.see(f"开启版本比较：最终值 = {results[True]}")
    r.see(f"关闭版本比较：最终值 = {results[False]}")
    ok = results[True] == "NEW" and results[False] == "OLD"
    r.done(ok, "“这行版本比较代码删掉会怎样？”——旧消息晚到时会把新值盖掉（对照组读到 OLD）。"
               "版本号由入口统一分配，不依赖各台机器的物理时钟，所以时钟不准也不影响这个比较。")
    return r


# ---------------------------------------------------------------- E4
def e4():
    r = Record("E4", "节点重启后丢失内存状态", "缓存允许单个节点丢数据；另一份副本仍可读")
    r.predict("重启首选节点后，它自己查不到这个 key；通过入口读取，首选节点回答“没有”，入口再问备份节点，仍能读到。"
              "重启的节点不会自动从别的节点补回数据。")
    with new_cluster() as c:
        k = key_on(0, "e4-")
        r.step(f"写入 {k}", gw_set(c, k, "keep-me", 0))
        c.restart_node(0)
        r.step("重启节点 0")
        a = node_get(c, 0, k)
        r.step("直接查节点 0", a)
        g = gw_get(c, k)
        r.step("通过入口读", g)
        a2 = node_get(c, 0, k)
        r.step("读完后再直接查节点 0", a2)
        r.see(f"节点 0 重启后 found={a.get('found')}")
        r.see(f"入口读到 value={g.get('value')}，来自节点 {g.get('from_node')}")
        r.see(f"入口读完后节点 0 仍然 found={a2.get('found')}（没有自动补齐）")
        ok = not a.get("found") and g.get("value") == "keep-me" and g.get("from_node") == 1 and not a2.get("found")
        r.done(ok, "数据只放在内存，进程重启就清空，这是我们为了简单做的取舍。入口仍能从备份节点读到。"
                   "但此时这个 key 只剩 1 份，第一版没有做“读修复”或后台同步，报告里要说明这是已知限制。")
    return r


# ---------------------------------------------------------------- E5
def e5():
    r = Record("E5", "某个 key 占据 50% 的读请求（热点）", "按 key 取模分片时，热点 key 会集中到它的首选节点")
    r.predict("均匀访问时 3 个节点收到的读请求大致相同；让 50% 的读集中到一个 key 后，这个 key 的首选节点会明显更忙，"
              "大约多承担 50% 的总读量。")
    N = 3000
    rows = []
    with new_cluster() as c:
        keys = [f"user:{i}" for i in range(300)]
        for k in keys:
            gw_set(c, k, "x", 0)
        hot = keys[0]
        hot_node = stable_hash(hot) % 3
        r.step(f"预先写入 300 个 key；热点 key = {hot}，它的首选节点是 {hot_node}")
        rnd = random.Random(42)
        for mode in ("uniform", "hotspot"):
            before = [call("GET", c.node_url(i) + "/internal/stats")["requests"]["get"] for i in range(3)]
            lat = []
            for _ in range(N):
                k = hot if (mode == "hotspot" and rnd.random() < 0.5) else rnd.choice(keys)
                t0 = time.perf_counter()
                gw_get(c, k)
                lat.append((time.perf_counter() - t0) * 1000)
            after = [call("GET", c.node_url(i) + "/internal/stats")["requests"]["get"] for i in range(3)]
            per = [after[i] - before[i] for i in range(3)]
            lat.sort()
            row = {"mode": mode, "requests": N, "node0": per[0], "node1": per[1], "node2": per[2],
                   "avg_ms": round(sum(lat) / N, 2), "p95_ms": round(lat[int(N * 0.95)], 2)}
            rows.append(row)
            r.step(f"{'均匀访问' if mode == 'uniform' else '50% 打到热点'}：发 {N} 次读，各节点收到 {per}")
    with open(os.path.join(RESULTS, "E5_hotspot.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    _plot_e5(rows, hot_node)
    u, h = rows
    share_u = u[f"node{hot_node}"] / N
    share_h = h[f"node{hot_node}"] / N
    r.see(f"均匀访问：节点 0/1/2 = {u['node0']}/{u['node1']}/{u['node2']}，平均延迟 {u['avg_ms']} ms，P95 {u['p95_ms']} ms")
    r.see(f"热点访问：节点 0/1/2 = {h['node0']}/{h['node1']}/{h['node2']}，平均延迟 {h['avg_ms']} ms，P95 {h['p95_ms']} ms")
    r.see(f"节点 {hot_node} 承担的读请求占比：{share_u:.0%} → {share_h:.0%}")
    ok = share_h > share_u + 0.3
    r.done(ok, "读取总是先问首选节点，所以热点 key 的读请求全部落在同一个节点上，取模分片无法分散单个 key 的压力。"
               "延迟在本机上差别不大，因为三个进程共用一台电脑，没有真正的网络和 CPU 瓶颈。"
               "可能的改进（第一版没有做）：热点 key 的读请求随机分给两个副本，或在入口加本地缓存。")
    return r


def _plot_e5(rows, hot_node):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    fig, ax = plt.subplots(figsize=(7, 4), dpi=150)
    x = [0, 1, 2]
    wdt = 0.36
    u = [rows[0][f"node{i}"] for i in x]
    h = [rows[1][f"node{i}"] for i in x]
    b1 = ax.bar([i - wdt / 2 for i in x], u, wdt, label="均匀访问", color="#8FC9E8")
    b2 = ax.bar([i + wdt / 2 for i in x], h, wdt, label="50% 打到热点 key", color="#2683C6")
    for bars in (b1, b2):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 15, str(int(b.get_height())),
                    ha="center", va="bottom", fontsize=9, color="#333333")
    ax.set_xticks(x)
    ax.set_xticklabels([f"节点 {i}" + ("\n(热点首选)" if i == hot_node else "") for i in x])
    ax.set_ylabel("收到的读请求数")
    ax.set_title(f"每种模式各发 {rows[0]['requests']} 次读请求")
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#E5E5E5")
    ax.set_axisbelow(True)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "E5_hotspot.png"))
    plt.close(fig)


# ---------------------------------------------------------------- E6
def e6():
    r = Record("E6", "TTL 到期失效", "带 TTL 的数据到期后读不到")
    r.predict("写入 TTL=2 秒的数据，1 秒时能读到，2.5 秒时读不到（返回 found=false，而不是错误）。")
    with new_cluster() as c:
        k = key_on(1, "e6-")
        r.step(f"写入 {k}，ttl_seconds=2", gw_set(c, k, "short-lived", 2))
        time.sleep(1)
        a = gw_get(c, k)
        r.step("1 秒后读", a)
        time.sleep(1.5)
        b = gw_get(c, k)
        r.step("2.5 秒后读", b)
        r.see(f"1 秒时 found={a.get('found')}；2.5 秒时 found={b.get('found')}，ok={b.get('ok')}")
        ok = a.get("found") and b.get("ok") and not b.get("found")
        r.done(ok, "节点在读取时检查到期时间，过期就删掉并当作不存在（惰性删除）。")
    return r


# ---------------------------------------------------------------- E7
def e7():
    r = Record("E7", "LRU 淘汰最久没用的数据", "节点条目数超过容量时，淘汰最久没被读写的那条")
    r.predict("把节点容量设为 3。写入 A、B、C 后读一次 A，再写 D：B 是最久没被用过的，应被淘汰；A、C、D 保留。")
    with new_cluster(capacity=3) as c:
        names = ["A", "B", "C", "D"]
        ks = {n: key_on(2, f"e7-{n}-") for n in names}
        for n in "ABC":
            r.step(f"写入 {n}", node_set(c, 2, {"key": ks[n], "value": n, "version": 1, "expire_at": 0}))
        r.step("读一次 A", node_get(c, 2, ks["A"]))
        r.step("写入 D", node_set(c, 2, {"key": ks["D"], "value": "D", "version": 1, "expire_at": 0}))
        found = {n: node_get(c, 2, ks[n]).get("found") for n in names}
        stats = call("GET", c.node_url(2) + "/internal/stats")
        r.see(f"各 key 是否还在：{found}")
        r.see(f"节点统计：size={stats['size']}，evicted={stats['evicted']}")
        ok = found == {"A": True, "B": False, "C": True, "D": True}
        r.done(ok, "节点用 OrderedDict 记录访问顺序，每次读写都把 key 移到末尾，满了就删掉最前面那条。")
    return r


# ---------------------------------------------------------------- E8
def e8():
    r = Record("E8", "系统时间突然倒退一分钟", "版本比较不依赖物理时钟；但 TTL 依赖物理时钟")
    r.predict("把节点时钟调慢 60 秒：(1) 版本比较照常工作，旧版本仍被拒绝；(2) 一条已经过期的数据会“复活”，又能读到。"
              "第 (2) 点是我们设计的已知限制。")
    with new_cluster() as c:
        k = key_on(0, "e8-")
        now = time.time()
        r.step("写入 version=2，1 秒后到期", node_set(c, 0, {"key": k, "value": "v2", "version": 2, "expire_at": now + 1}))
        r.step("把节点 0 的时钟调慢 60 秒", call("POST", c.node_url(0) + "/admin/clock", {"offset": -60}))
        old = node_set(c, 0, {"key": k, "value": "v1", "version": 1, "expire_at": 0})
        r.step("时钟倒退后，发来 version=1 的旧消息", old)
        time.sleep(1.5)
        g = node_get(c, 0, k)
        r.step("等 1.5 秒（按真实时间已过期）后查节点 0", g)
        r.see(f"旧版本是否被写入：applied={old.get('applied')}")
        r.see(f"真实时间已过期，但节点时钟慢了 60 秒，读取 found={g.get('found')}，value={g.get('value')}")
        ok = old.get("applied") is False and g.get("found") and g.get("value") == "v2"
        r.done(ok, "“为什么不能只用物理时钟解决冲突？”——如果用时间戳比较新旧，时钟倒退后新写入会被当成旧数据。"
                   "我们用入口分配的版本号，所以 (1) 不受影响。但 TTL 用的是物理时间，时钟倒退会让过期数据多活一会儿；"
                   "对缓存来说可以接受，但报告里要承认。")
    return r


# ---------------------------------------------------------------- E9
N_CONC, ROUNDS, ORIGIN_DELAY = 100, 5, 0.15


def _stampede_round(c, key):
    """等 key 过期后，100 个线程同时 /query，返回 (回源次数, 成功数, 延迟列表, 耗时)。"""
    import threading
    call("POST", c.gateway_url + "/debug/reset", {})
    barrier = threading.Barrier(N_CONC)
    lat, oks = [], []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        t0 = time.perf_counter()
        try:
            r = call("GET", c.gateway_url + "/query?key=" + quote(key, safe=""), timeout=15)
            ok = r.get("ok") and r.get("found")
        except OSError:
            ok = False
        with lock:
            lat.append((time.perf_counter() - t0) * 1000)
            oks.append(bool(ok))

    ts = [threading.Thread(target=worker) for _ in range(N_CONC)]
    t_start = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    elapsed = time.perf_counter() - t_start
    m = call("GET", c.gateway_url + "/debug/metrics")
    return m["origin_load_count"], sum(oks), sorted(lat), elapsed, m


def e9():
    r = Record("E9", "热点 key 过期后 100 个请求同时到达（缓存击穿）",
               "同一个 key 的并发未命中只回源一次（Singleflight，只在同一个入口进程内有效）")
    r.predict(f"热点 key demo:hot:1 刚过期时，{N_CONC} 个请求同时 /query。关闭 Singleflight：每个请求都各自回源，"
              f"回源次数接近 {N_CONC}；开启后：只有 1 个请求回源，其余等它的结果，回源次数约为 1。两种情况请求都应全部成功。")
    key = "demo:hot:1"
    rows = []
    for sf in (False, True):
        mode = "开启 Singleflight" if sf else "关闭 Singleflight"
        with new_cluster(singleflight=sf, origin_delay=ORIGIN_DELAY) as c:
            for rnd in range(1, ROUNDS + 1):
                # 先让缓存里有这个 key，再等它过期，确保这一轮开始时确实是“刚过期”
                call("GET", c.gateway_url + "/query?key=" + quote(key, safe=""), timeout=5)
                time.sleep(3.3)
                loads, okn, lat, elapsed, m = _stampede_round(c, key)
                row = {"singleflight": "on" if sf else "off", "round": rnd, "concurrency": N_CONC,
                       "origin_loads": loads, "success": okn, "waiters": m["coalesced_waiter_count"],
                       "avg_ms": round(sum(lat) / len(lat), 1), "p95_ms": round(lat[int(len(lat) * 0.95) - 1], 1),
                       "throughput_rps": round(N_CONC / elapsed, 1)}
                rows.append(row)
                r.step(f"[{mode}] 第 {rnd} 轮：回源 {loads} 次，成功 {okn}/{N_CONC}，等待者 {row['waiters']}，"
                       f"平均 {row['avg_ms']} ms，P95 {row['p95_ms']} ms")
    with open(os.path.join(RESULTS, "E9_stampede.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    def summary(flag):
        sel = [x for x in rows if x["singleflight"] == flag]
        loads = [x["origin_loads"] for x in sel]
        return {"loads": loads, "avg_loads": sum(loads) / len(loads),
                "success": sum(x["success"] for x in sel), "total": len(sel) * N_CONC,
                "avg_ms": sum(x["avg_ms"] for x in sel) / len(sel),
                "p95_ms": sum(x["p95_ms"] for x in sel) / len(sel)}

    off, on = summary("off"), summary("on")
    _plot_e9(off, on)
    r.see(f"关闭：每轮回源 {off['loads']}，平均 {off['avg_loads']:.1f} 次；成功 {off['success']}/{off['total']}；"
          f"平均延迟 {off['avg_ms']:.0f} ms，P95 {off['p95_ms']:.0f} ms")
    r.see(f"开启：每轮回源 {on['loads']}，平均 {on['avg_loads']:.1f} 次；成功 {on['success']}/{on['total']}；"
          f"平均延迟 {on['avg_ms']:.0f} ms，P95 {on['p95_ms']:.0f} ms")
    ok = (on["success"] == on["total"] and off["success"] == off["total"]
          and max(on["loads"]) <= 2 and off["avg_loads"] >= N_CONC * 0.5)
    r.done(ok, "关闭时，大家同时发现缓存没了，于是各自去查后端，后端在一瞬间收到几十上百次一样的查询，这就是缓存击穿。"
               "开启后，第一个请求负责回源，其他请求等它的结果，后端只被查了一次。"
               "注意：这只在同一个入口进程内有效；如果以后部署多个入口，需要跨进程的锁。"
               "开启 Singleflight 也不能解决缓存雪崩（大量不同 key 同时过期）和缓存穿透（查本来就不存在的数据）。")
    return r


def _plot_e9(off, on):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    fig, ax = plt.subplots(figsize=(7, 4), dpi=150)
    rounds = list(range(1, len(off["loads"]) + 1))
    w = 0.36
    b1 = ax.bar([x - w / 2 for x in rounds], off["loads"], w, label="关闭 Singleflight", color="#8FC9E8")
    b2 = ax.bar([x + w / 2 for x in rounds], on["loads"], w, label="开启 Singleflight", color="#2683C6")
    for bars in (b1, b2):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 1.5, str(int(b.get_height())),
                    ha="center", va="bottom", fontsize=9, color="#333333")
    ax.set_xticks(rounds)
    ax.set_xticklabels([f"第 {x} 轮" for x in rounds])
    ax.set_ylabel("后端（模拟数据源）被查询的次数")
    ax.set_title(f"热点 key 刚过期，{N_CONC} 个请求同时到达")
    ax.set_ylim(0, N_CONC * 1.3)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#E5E5E5")
    ax.set_axisbelow(True)
    ax.legend(frameon=False, loc="upper center", ncol=2)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "E9_stampede.png"))
    plt.close(fig)


# ---------------------------------------------------------------- E10
def e10():
    r = Record("E10", "回填后在 TTL 内再次访问（缓存命中）", "回源拿到的数据会写回缓存，TTL 内的后续请求直接命中，不再回源")
    r.predict("第一次 /query 未命中，回源 1 次并写回（TTL 3 秒）。紧接着 100 个并发 /query 全部命中缓存，回源次数仍是 1。"
              "等 3 秒过期后再查一次，会重新回源，回源次数变成 2。")
    key = "demo:hot:2"
    with new_cluster(singleflight=True, origin_delay=ORIGIN_DELAY) as c:
        call("POST", c.gateway_url + "/debug/reset", {})
        first = call("GET", c.gateway_url + "/query?key=" + quote(key, safe=""), timeout=5)
        r.step(f"第一次 /query {key}", first)
        loads, okn, lat, elapsed, m = _stampede_round_keep(c, key)
        r.step(f"紧接着 {N_CONC} 个并发 /query：成功 {okn}，命中缓存 {m['cache_hit_count']} 次，累计回源 {m['origin_load_count']} 次")
        time.sleep(3.3)
        again = call("GET", c.gateway_url + "/query?key=" + quote(key, safe=""), timeout=5)
        m2 = call("GET", c.gateway_url + "/debug/metrics")
        r.step("3.3 秒后（已过期）再查一次", again)
        r.see(f"第一次来源：{first.get('source')}，cache_filled={first.get('cache_filled')}")
        r.see(f"TTL 内 {N_CONC} 个请求：命中 {m['cache_hit_count']} 次，回源总数仍为 {m['origin_load_count']}")
        r.see(f"过期后再查：来源 {again.get('source')}，回源总数变为 {m2['origin_load_count']}")
        ok = (first.get("source") == "origin" and first.get("cache_filled") and okn == N_CONC
              and m["cache_hit_count"] == N_CONC and m["origin_load_count"] == 1
              and again.get("source") == "origin" and m2["origin_load_count"] == 2)
        r.done(ok, "回源后会用原来的 /set 流程写回两个副本，所以 TTL 内的请求都从缓存拿。"
                   "回填的 TTL 是 3 秒，过期后第一个请求会重新回源，这正是 E9 里要防的那个时刻。")
    return r


def _stampede_round_keep(c, key):
    """和 _stampede_round 一样并发查询，但不清零计数器。"""
    import threading
    barrier = threading.Barrier(N_CONC)
    lat, oks = [], []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        t0 = time.perf_counter()
        try:
            res = call("GET", c.gateway_url + "/query?key=" + quote(key, safe=""), timeout=15)
            ok = res.get("ok") and res.get("found")
        except OSError:
            ok = False
        with lock:
            lat.append((time.perf_counter() - t0) * 1000)
            oks.append(bool(ok))

    ts = [threading.Thread(target=worker) for _ in range(N_CONC)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    m = call("GET", c.gateway_url + "/debug/metrics")
    return m["origin_load_count"], sum(oks), sorted(lat), time.perf_counter() - t0, m


EXPERIMENTS = {"E1": e1, "E1b": e1b, "E2": e2, "E3": e3, "E4": e4, "E5": e5, "E6": e6, "E7": e7, "E8": e8,
               "E9": e9, "E10": e10}


def main():
    os.makedirs(RESULTS, exist_ok=True)
    chosen = sys.argv[1:] or list(EXPERIMENTS)
    records = []
    for eid in chosen:
        records.append(EXPERIMENTS[eid]())
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"# 故障注入实验记录", "", f"运行时间：{stamp}　　运行环境：单机，3 个节点进程 + 1 个入口进程", "",
             "| 实验 | 内容 | 对照预测 |", "|---|---|---|"]
    lines += [f"| {x.eid} | {x.title} | {'符合' if x.passed else '不符'} |" for x in records]
    lines += [""] + [x.markdown() for x in records]
    name = "experiment_log.md" if not sys.argv[1:] else f"experiment_log_{'_'.join(chosen)}.md"
    with open(os.path.join(RESULTS, name), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n记录已保存：experiments/results/{name}")
    print("汇总：" + "，".join(f"{x.eid} {'符合' if x.passed else '不符'}" for x in records))
    sys.exit(0 if all(x.passed for x in records) else 1)


if __name__ == "__main__":
    main()
