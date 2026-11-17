"""ChronicleStore 单元测试 —— 仅依赖标准库, 无需安装 astrbot。

运行: python -m pytest tests/test_store.py -v
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.store import ChronicleStore


def make_entry(
    umo="10001",
    group_name="测试群",
    iso_year=2026,
    iso_week=10,
    newest_msg_ts=1000.0,
    **extra,
):
    entry = {
        "umo": umo,
        "group_name": group_name,
        "iso_year": iso_year,
        "iso_week": iso_week,
        "created_at": 1700000000.0,
        "newest_msg_ts": newest_msg_ts,
        "chronicle_text": f"第 {iso_year}-W{iso_week} 周的史记",
        "biographies": [{"name": "张三", "summary": "测试"}],
        "image_path": None,
        "version": 0,
    }
    entry.update(extra)
    return entry


def test_save_get_roundtrip(tmp_path):
    store = ChronicleStore(str(tmp_path))
    entry = make_entry()
    store.save(entry)

    got = store.get("10001", 2026, 10)
    assert got is not None
    assert got["umo"] == "10001"
    assert got["iso_year"] == 2026
    assert got["iso_week"] == 10
    assert got["chronicle_text"] == entry["chronicle_text"]
    assert got["biographies"] == entry["biographies"]
    assert got["image_path"] is None
    # 首次保存 version = 1
    assert got["version"] == 1
    # 文件真实落盘
    assert (tmp_path / "chronicles.json").exists()


def test_save_twice_increments_version(tmp_path):
    store = ChronicleStore(str(tmp_path))
    store.save(make_entry())
    store.save(make_entry())

    got = store.get("10001", 2026, 10)
    assert got["version"] == 2
    # 覆盖最新版, 不保留旧版
    assert store.list("10001") == [got]


def test_should_skip(tmp_path):
    store = ChronicleStore(str(tmp_path))
    store.save(make_entry(newest_msg_ts=1000.0))

    # 最新消息时间戳 >= 上次生成时的时间戳 → 跳过
    assert store.should_skip("10001", 2026, 10, 1000.0) is True
    assert store.should_skip("10001", 2026, 10, 999.0) is True
    # 出现了更新的消息 → 不跳过
    assert store.should_skip("10001", 2026, 10, 1000.5) is False
    # 未生成过的周 → 不跳过
    assert store.should_skip("10001", 2026, 11, 2000.0) is False
    # 其他群不受影响
    assert store.should_skip("99999", 2026, 10, 0.0) is False


def test_list_sorted_desc(tmp_path):
    store = ChronicleStore(str(tmp_path))
    store.save(make_entry(iso_year=2026, iso_week=5))
    store.save(make_entry(iso_year=2026, iso_week=3))
    store.save(make_entry(iso_year=2025, iso_week=52))
    store.save(make_entry(iso_year=2026, iso_week=10))
    # 另一个群的数据不应混入
    store.save(make_entry(umo="20002", iso_year=2026, iso_week=1))

    weeks = [(e["iso_year"], e["iso_week"]) for e in store.list("10001")]
    assert weeks == [(2026, 10), (2026, 5), (2026, 3), (2025, 52)]


def test_get_by_id(tmp_path):
    store = ChronicleStore(str(tmp_path))
    store.save(make_entry(iso_year=2026, iso_week=3))
    store.save(make_entry(iso_year=2026, iso_week=10))
    store.save(make_entry(iso_year=2026, iso_week=5))

    assert store.get_by_id("10001", 1)["iso_week"] == 10  # 最新在前
    assert store.get_by_id("10001", 2)["iso_week"] == 5
    assert store.get_by_id("10001", 3)["iso_week"] == 3
    assert store.get_by_id("10001", 0) is None
    assert store.get_by_id("10001", 4) is None
    assert store.get_by_id("20002", 1) is None


def test_corrupt_json_recovers(tmp_path):
    data_file = tmp_path / "chronicles.json"
    data_file.write_text("这不是 JSON {{{", encoding="utf-8")

    store = ChronicleStore(str(tmp_path))
    assert store.get("10001", 2026, 10) is None  # 损坏时初始化为空, 不崩溃

    original = data_file.read_bytes()
    with pytest.raises(RuntimeError):
        store.save(make_entry())
    assert data_file.read_bytes() == original


def test_missing_file_and_no_tmp_left(tmp_path):
    store = ChronicleStore(str(tmp_path / "nested" / "dir"))
    assert store.list("10001") == []
    store.save(make_entry())
    # 原子写入后 tmp 文件不应残留
    assert not list((tmp_path / "nested" / "dir").glob("*.tmp"))


def test_reload_before_write_merge(tmp_path):
    """并发安全: 两个实例各自保存, 后写者 reload 后合并, 不丢前者数据。"""
    store_a = ChronicleStore(str(tmp_path))
    store_b = ChronicleStore(str(tmp_path))
    store_a.save(make_entry(iso_year=2026, iso_week=10))
    store_b.save(make_entry(iso_year=2026, iso_week=11))

    # 重新加载新实例, 两个条目都在
    store_c = ChronicleStore(str(tmp_path))
    assert store_c.get("10001", 2026, 10) is not None
    assert store_c.get("10001", 2026, 11) is not None
