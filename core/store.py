"""群史记 ChronicleStore —— JSON 持久化存储。

条目格式（caller 构造，store 只负责存取）:
    {
        "umo": str,
        "group_name": str,
        "iso_year": int,
        "iso_week": int,
        "created_at": float,       # time.time() epoch
        "newest_msg_ts": float,    # 生成时最新消息时间戳
        "chronicle_text": str,
        "biographies": list[dict],
        "image_path": str | None,
        "version": int,            # 每次覆盖保存自动 +1
    }

写入策略: reload-before-write(合并) + tmp 文件 + os.replace 原子替换,
保证并发场景下不丢数据、不产生半截文件。
"""

import json
import os
import tempfile
import threading

from astrbot.api import logger


class ChronicleStore:
    _locks: dict[str, threading.RLock] = {}

    def __init__(self, data_dir: str):
        self._file = os.path.join(data_dir, "chronicles.json")
        os.makedirs(data_dir, exist_ok=True)
        self._lock = self._locks.setdefault(
            os.path.abspath(self._file), threading.RLock()
        )
        self._load_error = None
        self._data: dict = self._load()

    # ---------- 内部: 加载 / 写入 ----------

    def _load(self) -> dict:
        """从磁盘加载数据; 文件缺失或损坏时返回空 dict, 绝不抛异常。"""
        self._load_error = None
        if not os.path.exists(self._file):
            return {}
        try:
            with open(self._file, encoding="utf-8-sig") as f:
                raw = json.load(f)
        except Exception as e:
            self._load_error = str(e)
            logger.error("Chronicle storage could not be read; writes are disabled")
            return {}
        if not isinstance(raw, dict):
            self._load_error = "Expected a JSON object"
            logger.error("Invalid chronicle storage; writes are disabled")
            return {}
        return raw

    def _write_atomic(self) -> None:
        """写 tmp 文件后原子替换, 避免半截文件。"""
        if self._load_error:
            raise RuntimeError(
                "史记存储读取失败，已禁止覆盖，请管理员检查 chronicles.json"
            )
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=os.path.dirname(self._file),
                prefix="chronicles-",
                suffix=".tmp",
                delete=False,
            ) as f:
                tmp = f.name
                json.dump(self._data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._file)
        finally:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)

    def clear_memory(self) -> None:
        """Release the in-memory legacy dict after successful SQLite migration."""
        with self._lock:
            self._data.clear()

    # ---------- 键 ----------

    def _key(self, umo: str, iso_year: int, iso_week: int) -> str:
        return f"{umo}|{iso_year}-W{iso_week:02d}"

    # ---------- 对外 API ----------

    def save(self, entry: dict) -> None:
        """保存/覆盖一个条目; 已存在同键时 version +1, 否则 version=1。"""
        # 先重载磁盘数据再合并, 防止并发时覆盖其他实例刚写入的条目
        with self._lock:
            self._data = self._load()
            if self._load_error:
                raise RuntimeError(
                    "史记存储读取失败，已禁止覆盖，请管理员检查 chronicles.json"
                )
            key = self._key(entry["umo"], entry["iso_year"], entry["iso_week"])
            old = self._data.get(key)
            old_version = old.get("version", 0) if isinstance(old, dict) else 0
            entry = dict(entry)
            entry["version"] = old_version + 1
            self._data[key] = entry
            self._write_atomic()

    def mark_delivered(self, umo: str, text: str) -> bool:
        """Record successful delivery without creating a new content revision.

        Args:
            umo: Session that received the result.
            text: Exact generated content; mismatched revisions are not updated.

        Returns:
            Whether a matching pending result was marked delivered.
        """
        with self._lock:
            self._data = self._load()
            if self._load_error:
                raise RuntimeError(
                    "史记存储读取失败，已禁止覆盖，请管理员检查 chronicles.json"
                )
            for entry in self.list(umo):
                if (
                    entry.get("chronicle_text") == text
                    and entry.get("delivery_complete") is False
                ):
                    entry["delivery_complete"] = True
                    self._write_atomic()
                    return True
        return False

    def get(self, umo: str, iso_year: int, iso_week: int) -> dict | None:
        return self._data.get(self._key(umo, iso_year, iso_week))

    def list(self, umo: str) -> list[dict]:
        """某群的全部条目, 按 (iso_year, iso_week) 降序; 返回与保存时相同结构的 dict。"""
        prefix = f"{umo}|"
        entries: list[dict] = []
        for key, value in self._data.items():
            if not key.startswith(prefix):
                continue
            if isinstance(value, dict) and "iso_year" in value and "iso_week" in value:
                entries.append(value)
        entries.sort(
            key=lambda e: (e.get("iso_year", 0), e.get("iso_week", 0)),
            reverse=True,
        )
        return entries

    def get_by_id(self, umo: str, idx: int) -> dict | None:
        """idx 为 1 起始下标(list 降序, 最新在前); 越界返回 None。"""
        entries = self.list(umo)
        if idx < 1 or idx > len(entries):
            return None
        return entries[idx - 1]

    def should_skip(
        self, umo: str, iso_year: int, iso_week: int, newest_ts: float
    ) -> bool:
        """该周已生成且生成后没有新消息 → True(跳过)。"""
        entry = self.get(umo, iso_year, iso_week)
        if entry is None:
            return False
        if entry.get("delivery_complete", True) is False:
            return False
        return entry.get("newest_msg_ts", 0.0) >= newest_ts
