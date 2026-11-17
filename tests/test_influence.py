"""influence 模块单元测试 —— 仅依赖标准库, 无需安装 astrbot。

运行: python -m pytest tests/test_influence.py -v
"""

import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import core.influence as inf


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _clear_probe_cache():
    """每个用例前清空探测缓存, 防止跨用例 id() 复用串扰。"""
    inf._clear_probe_cache()
    yield


class FakeMessage:
    """模拟 LivingMemory Message: sender_id / sender_name / timestamp。"""

    def __init__(self, sender_id, sender_name="", timestamp=0.0):
        self.sender_id = sender_id
        self.sender_name = sender_name
        self.timestamp = timestamp


def make_evidence(stats=None, msgs=None, bio_msgs=None):
    return SimpleNamespace(
        stats=stats or {},
        chronicle_msgs=msgs or [],
        biography_msgs=bio_msgs or [],
    )


def now():
    return time.time()


class FakeStar:
    """模拟 AstrBot 注册表中的 StarHandler。"""

    def __init__(self, instance, name=None, module_path=None, activated=True):
        self.star_cls = instance
        self.activated = activated
        self.name = name or type(instance).__name__
        self.display_name = self.name
        self.root_dir_name = self.name
        self.module_path = module_path or self.name.lower()


class FakeContext:
    """模拟 AstrBot Context: get_registered_star / get_all_stars。"""

    def __init__(self, stars=None, registered=None):
        self._stars = stars or []
        self._registered = registered or {}

    def get_registered_star(self, name):
        return self._registered.get(name)

    def get_all_stars(self):
        return list(self._stars)


def no_star_context():
    return SimpleNamespace(get_all_stars=lambda: [])


class FakeInfluenceAnalyzer:
    """模拟 self_learning SocialGraphAnalyzer.get_influence_ranking。"""

    def __init__(self, result, error=None):
        self._result = result
        self._error = error
        self.calls = []

    async def get_influence_ranking(self, group_id, top_k=10):
        self.calls.append((group_id, top_k))
        if self._error is not None:
            raise self._error
        return list(self._result)


class FakeRAStore:
    """模拟 relation_analyzer RelationStore.overview。"""

    def __init__(self, centers=None, users=None):
        self._centers = centers or []
        self._users = users or {}
        self.calls = []

    async def overview(self, group_id, *, top_limit=200, daily_decay=1.0):
        self.calls.append(group_id)
        return {
            "user_count": len(self._users),
            "relation_count": 0,
            "centers": list(self._centers),
            "active_users": [],
            "users": dict(self._users),
        }


def make_sl_star(analyzer):
    plugin = SimpleNamespace(social_graph_analyzer=analyzer)
    return FakeStar(plugin, name="astrbot_plugin_self_learning")


def make_ra_star(store):
    plugin = SimpleNamespace(store=store)
    return FakeStar(plugin, name="astrbot_plugin_group_relation_analyzer")


# ---------- 1. 仅 LM 来源 → 权重重归一化为 lm=1.0 ----------


def test_lm_only_renormalizes_weights():
    stats = {"a": 20, "b": 10}
    msgs = [
        FakeMessage("a", "Alice", now()),
        FakeMessage("a", "Alice", now() - 86400),
        FakeMessage("b", "Bob", now()),
    ]
    ev = make_evidence(stats, msgs)
    result = run(inf.rank_influential(ev, no_star_context(), "qq:1:group:g1", top_k=3))

    assert len(result) == 2
    assert result[0]["sender_id"] == "a"
    assert result[0]["score"] == pytest.approx(1.0)
    assert set(result[0]["breakdown"]) == {"lm"}
    assert result[1]["sender_id"] == "b"
    assert result[1]["score"] == pytest.approx(0.0)
    assert set(result[1]["breakdown"]) == {"lm"}


# ---------- 2. 三来源融合, 加权求和 + min-max, 排序正确 ----------


def test_three_sources_fusion():
    stats = {"u1": 10, "u2": 4, "u3": 2}
    msgs = [
        FakeMessage("u1", "N1", now()),
        FakeMessage("u1", "N1", now() - 86400),
        FakeMessage("u2", "N2", now()),
        FakeMessage("u3", "N3", now()),
    ]
    ev = make_evidence(stats, msgs)

    sl_analyzer = FakeInfluenceAnalyzer(
        [
            {"user_id": "u2", "pagerank": 0.5},
            {"user_id": "u1", "pagerank": 0.3},
            {"user_id": "u4", "pagerank": 0.2},
        ]
    )
    ra_store = FakeRAStore(centers=[("u3", 8.0), ("u1", 2.0)])
    ctx = FakeContext(stars=[make_sl_star(sl_analyzer), make_ra_star(ra_store)])

    result = run(inf.rank_influential(ev, ctx, "qq:1:group:g1", top_k=3))

    assert [r["sender_id"] for r in result] == ["u1", "u2", "u3"]
    assert result[0]["score"] == pytest.approx(
        0.5 * 1.0 + 0.3 * (1 / 3) + 0.2 * 0.0, abs=1e-3
    )
    assert set(result[0]["breakdown"]) == {"lm", "sl", "ra"}
    assert result[1]["score"] > result[2]["score"]
    # u4 来自 SL 但 lm/ra 无值 → 分最低, 被 top_k 截断
    assert "u4" not in [r["sender_id"] for r in result]


# ---------- 3. SL 星在但 ranking 为空 → 视为缺失, 不抛异常 ----------


def test_sl_empty_ranking_missing():
    stats = {"a": 5, "b": 3}
    msgs = [FakeMessage("a", "A", now()), FakeMessage("b", "B", now())]
    ev = make_evidence(stats, msgs)

    analyzer = FakeInfluenceAnalyzer([])
    ctx = FakeContext(stars=[make_sl_star(analyzer)])

    result = run(inf.rank_influential(ev, ctx, "qq:1:group:g1"))
    assert len(result) == 2
    assert all(set(r["breakdown"]) == {"lm"} for r in result)


# ---------- 4. RA 能力缺失 → 来源缺失, 仅 LM 排行 ----------


def test_ra_capability_missing():
    stats = {"a": 5, "b": 3}
    msgs = [FakeMessage("a", "A", now()), FakeMessage("b", "B", now())]
    ev = make_evidence(stats, msgs)

    disabled_ra = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": False,
            "ready": False,
            "capabilities": {"relations": True},
        },
        get_relations_overview=lambda gid, **kw: {
            "centers": [{"user_id": "a", "score": 99}]
        },
    )
    ctx = FakeContext(
        stars=[FakeStar(disabled_ra, name="astrbot_plugin_relation_analyzer")]
    )
    result = run(inf.rank_influential(ev, ctx, "qq:1:group:g1"))
    assert len(result) == 2
    assert all(set(r["breakdown"]) == {"lm"} for r in result)


# ---------- 5. RA 公开接口异常 → 来源缺失, 不抛异常 ----------


def test_ra_public_error():
    stats = {"a": 5}
    msgs = [FakeMessage("a", "A", now())]
    ev = make_evidence(stats, msgs)

    bad_ra = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": True,
            "capabilities": {"relations": True},
        },
        get_relations_overview=lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("ra failed")
        ),
    )
    ctx = FakeContext(stars=[FakeStar(bad_ra, name="astrbot_plugin_relation_analyzer")])
    result = run(inf.rank_influential(ev, ctx, "qq:1:group:g1"))
    assert len(result) == 1
    assert result[0]["sender_id"] == "a"
    assert result[0]["score"] == pytest.approx(1.0)


# ---------- 6. 单人群 (max==min) → 1.0, 无 ZeroDivisionError ----------


def test_single_member_no_zerodiv():
    ev = make_evidence(
        {"solo": 7},
        [FakeMessage("solo", "S", now()), FakeMessage("solo", "S", now() - 86400)],
    )
    result = run(inf.rank_influential(ev, no_star_context(), "qq:1:group:g1"))
    assert len(result) == 1
    assert result[0]["score"] == pytest.approx(1.0)


# ---------- 7. 全员计数相等 → 全 1.0, 无崩溃 ----------


def test_all_equal_counts_all_one():
    ev = make_evidence(
        {"u1": 3, "u2": 3},
        [FakeMessage("u1", "A", now()), FakeMessage("u2", "B", now())],
    )
    result = run(inf.rank_influential(ev, no_star_context(), "qq:1:group:g1"))
    assert len(result) == 2
    assert {r["score"] for r in result} == {1.0}


# ---------- 8. bot id 从输出中排除 ----------


def test_bot_ids_excluded():
    stats = {"bot": 100, None: 40, "u1": 10, "u2": 8}
    msgs = [
        FakeMessage("u1", "Alice", now()),
        FakeMessage("u2", "Bob", now()),
        FakeMessage("bot", "Bot", now()),
        FakeMessage(None, "X", now()),
    ]
    ev = make_evidence(stats, msgs)
    result = run(inf.rank_influential(ev, no_star_context(), "qq:1:group:g1"))
    assert {r["sender_id"] for r in result} == {"u1", "u2"}


# ---------- 9. top_k 限制生效且降序 ----------


def test_top_k_respects_limit():
    stats = {"u1": 10, "u2": 8, "u3": 6, "u4": 4, "u5": 2}
    msgs = [FakeMessage(f"u{i}", "", now() - i * 100) for i in range(1, 6)]
    ev = make_evidence(stats, msgs)
    result = run(inf.rank_influential(ev, no_star_context(), "qq:1:group:g1", top_k=3))
    assert len(result) == 3
    scores = [r["score"] for r in result]
    assert scores == sorted(scores, reverse=True)


# ---------- 10. extract_group_id 解析 ----------


def test_extract_group_id():
    assert inf.extract_group_id("qq:123:group:456") == "456"
    assert inf.extract_group_id("aiocqhttp:GroupMessage:777") == "777"
    assert inf.extract_group_id("default:GroupMessage:bot123_456789") == "456789"
    assert inf.extract_group_id("weird") == "weird"
    assert inf.extract_group_id("a:b:c") == "c"
    assert inf.extract_group_id("") == ""
    assert inf.extract_group_id(None) == ""


# ---------- 11. 自定义权重 + 仅 LM → lm 重归一化为 1.0 ----------


def test_custom_weights_lm_only():
    stats = {"a": 10, "b": 6}
    msgs = [
        FakeMessage("a", "A", now()),
        FakeMessage("a", "A", now()),
        FakeMessage("b", "B", now()),
    ]
    ev = make_evidence(stats, msgs)
    result = run(
        inf.rank_influential(
            ev,
            no_star_context(),
            "qq:1:group:g1",
            weights={"lm": 0.8, "sl": 0.1, "ra": 0.1},
        )
    )
    assert result[0]["score"] == pytest.approx(1.0)
    assert result[1]["score"] == pytest.approx(0.0)


# ---------- 12. sender_name 从消息解析, 缺失回退 sender_id ----------


def test_sender_name_resolution():
    stats = {"u1": 5, "u2": 3}
    msgs = [
        FakeMessage("u1", "Alice", now()),
        FakeMessage("u1", "Alice", now() - 86400),
        FakeMessage("u2", "", now()),
    ]
    ev = make_evidence(stats, msgs)
    result = run(inf.rank_influential(ev, no_star_context(), "qq:1:group:g1"))
    names = {r["sender_id"]: r["sender_name"] for r in result}
    assert names["u1"] == "Alice"
    assert names["u2"] == "u2"


# ---------- 13. RA 公开能力接口有数据 → 参与排行 ----------


def test_ra_capability_usable():
    ra_plugin = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": True,
            "capabilities": {"relations": True},
        },
        get_relations_overview=lambda gid, **kw: {
            "centers": [
                {"user_id": "u1", "score": 10.0},
                {"user_id": "u2", "score": 5.0},
                {"user_id": "u3", "score": 0.0},
            ]
        },
    )
    ctx = FakeContext(stars=[FakeStar(ra_plugin, name="custom_relations_provider")])
    # LM 缺失 → 仅 RA 来源
    ev = make_evidence({}, [FakeMessage(uid, uid, now()) for uid in ("u1", "u2", "u3")])
    result = run(
        inf.rank_influential(ev, ctx, "qq:1:group:g1", weights={"ra": 1, "lm": 0})
    )

    assert [r["sender_id"] for r in result] == ["u1", "u2", "u3"]
    assert result[0]["score"] == pytest.approx(1.0)
    assert result[1]["score"] == pytest.approx(0.5)
    assert result[2]["score"] == pytest.approx(0.0)
    assert all("ra" in r["breakdown"] for r in result)


# ---------- 14. 全部来源缺失 → 空列表 ----------


def test_all_sources_missing_empty():
    ev = make_evidence({}, [])
    assert run(inf.rank_influential(ev, no_star_context(), "qq:1:group:g1")) == []


# ---------- 15. SL 分析器抛异常 → 来源缺失, 不抛异常 ----------


def test_sl_analyzer_raises():
    stats = {"a": 5, "b": 3}
    msgs = [FakeMessage("a", "A", now()), FakeMessage("b", "B", now())]
    ev = make_evidence(stats, msgs)

    analyzer = FakeInfluenceAnalyzer([], error=RuntimeError("boom"))
    ctx = FakeContext(stars=[make_sl_star(analyzer)])

    result = run(inf.rank_influential(ev, ctx, "qq:1:group:g1"))
    assert len(result) == 2
    assert all(set(r["breakdown"]) == {"lm"} for r in result)


# ---------- 16. SL 分析器经 services 容器发现 ----------


def test_sl_found_via_services_container():
    stats = {"a": 5, "b": 3}
    msgs = [FakeMessage("a", "A", now()), FakeMessage("b", "B", now())]
    ev = make_evidence(stats, msgs)

    analyzer = FakeInfluenceAnalyzer([{"user_id": "a", "pagerank": 0.9}])
    plugin = SimpleNamespace(
        services={"social": SimpleNamespace(social_graph_analyzer=analyzer)}
    )
    ctx = FakeContext(stars=[FakeStar(plugin, name="astrbot_plugin_self_learning")])

    result = run(inf.rank_influential(ev, ctx, "qq:1:group:g1"))
    assert len(result) == 2
    assert result[0]["sender_id"] == "a"
    assert set(result[0]["breakdown"]) == {"lm", "sl"}
    assert set(result[1]["breakdown"]) == {"lm", "sl"}


# ---------- 17. RA 进程内 overview 数据参与排行 ----------


def test_ra_in_process_used():
    stats = {"u1": 10, "u2": 4, "u3": 2}
    msgs = [
        FakeMessage("u1", "N1", now()),
        FakeMessage("u1", "N1", now() - 86400),
        FakeMessage("u2", "N2", now()),
        FakeMessage("u3", "N3", now()),
    ]
    ev = make_evidence(stats, msgs)

    ra_store = FakeRAStore(centers=[("u3", 8.0), ("u1", 2.0)])
    ctx = FakeContext(stars=[make_ra_star(ra_store)])

    result = run(inf.rank_influential(ev, ctx, "qq:1:group:g1"))
    # RA 单独权重 0.2; u3 在 RA 中最高但 LM 最低
    assert result[0]["sender_id"] == "u1"
    assert set(result[0]["breakdown"]) == {"lm", "ra"}
    assert "sl" not in result[0]["breakdown"]
    # 进程内 store 被实际调用过
    assert ra_store.calls == ["g1"]


# ---------- 18. relation_pairs (ABC 批次: 关系弧公开能力数据源) ----------


def test_relation_pairs_sorted_desc_and_bots_skipped():
    ts = now()
    ra_plugin = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": True,
            "capabilities": {"relations": True},
        },
        get_relation_pairs=lambda **kw: [
            {"group_id": "g1", "a": "u1", "b": "u2", "score": 3.0, "last_ts": ts},
            {"group_id": "g1", "a": "u1", "b": "u3", "score": 9.0, "last_ts": ts},
            {"group_id": "g1", "a": "bot", "b": "u4", "score": 99.0, "last_ts": ts},
            {"group_id": "g1", "a": "", "b": "u5", "score": 5.0, "last_ts": ts},
            {"group_id": "g2", "a": "x1", "b": "x2", "score": 1.0, "last_ts": ts},
        ],
    )
    ctx = FakeContext(
        stars=[FakeStar(ra_plugin, name="astrbot_plugin_relation_analyzer")]
    )
    pairs = run(inf.relation_pairs(ctx, "qq:1:group:g1", top_k=5))
    assert pairs == [("u1", "u3", 9.0), ("u1", "u2", 3.0)]


def test_relation_pairs_top_k_respected():
    ra_plugin = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": True,
            "capabilities": {"relations": True},
        },
        get_relation_pairs=lambda **kw: [
            (f"u{i}", f"v{i}", float(i)) for i in range(1, 8)
        ],
    )
    ctx = FakeContext(
        stars=[FakeStar(ra_plugin, name="astrbot_plugin_relation_analyzer")]
    )
    pairs = run(inf.relation_pairs(ctx, "qq:1:group:g1", top_k=3))
    assert len(pairs) == 3
    assert pairs[0][2] == 7.0


def test_relation_pairs_missing_plugin_returns_none():
    assert run(inf.relation_pairs(no_star_context(), "qq:1:group:g1")) is None


def test_relation_pairs_error_returns_none():
    ra_plugin = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": True,
            "capabilities": {"relations": True},
        },
        get_relation_pairs=lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    ctx = FakeContext(
        stars=[FakeStar(ra_plugin, name="astrbot_plugin_relation_analyzer")]
    )
    assert run(inf.relation_pairs(ctx, "qq:1:group:g1")) is None


def test_relation_pairs_no_rows_returns_none():
    ra_plugin = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": True,
            "capabilities": {"relations": True},
        },
        get_relation_pairs=lambda **kw: {"pairs": []},
    )
    ctx = FakeContext(
        stars=[FakeStar(ra_plugin, name="astrbot_plugin_relation_analyzer")]
    )
    assert run(inf.relation_pairs(ctx, "qq:1:group:g1")) is None


# ---------- 19. 去深层 BFS 反射探测与公开能力清单门控 ----------


def test_find_influence_analyzer_skips_private_deep_attrs():
    """私有嵌套属性不参与探测, 公开 services/analyzer 路径正常识别。"""
    hidden_analyzer = FakeInfluenceAnalyzer([{"user_id": "u1", "score": 1.0}])
    opaque_plugin = SimpleNamespace(
        _private=SimpleNamespace(nested=SimpleNamespace(analyzer=hidden_analyzer))
    )
    assert inf._find_influence_analyzer(opaque_plugin) is None

    public_analyzer = FakeInfluenceAnalyzer([{"user_id": "u1", "score": 2.0}])
    service_plugin = SimpleNamespace(services=SimpleNamespace(social=public_analyzer))
    assert inf._find_influence_analyzer(service_plugin) is public_analyzer


def test_capability_manifest_skips_disabled_or_unready():
    """未启用或未就绪的能力声明应被跳过, 继续选中就绪提供者。"""
    unready = FakeInfluenceAnalyzer([{"user_id": "u1", "score": 99.0}])
    ready = FakeInfluenceAnalyzer([{"user_id": "u2", "score": 10.0}])
    plugin_unready = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": False,
            "capabilities": {"social": True},
        },
        get_influence_ranking=unready.get_influence_ranking,
    )
    plugin_ready = SimpleNamespace(
        get_dynamics_capabilities=lambda: {
            "enabled": True,
            "ready": True,
            "capabilities": {"social": True},
        },
        get_influence_ranking=ready.get_influence_ranking,
    )
    ctx = FakeContext(
        stars=[
            FakeStar(plugin_unready, name="disabled_sl"),
            FakeStar(plugin_ready, name="ready_sl"),
        ]
    )
    resolved = inf._resolve_star_instance(
        ctx, inf._SL_ALIASES, capability_names=("social", "learning")
    )
    assert resolved is plugin_ready
