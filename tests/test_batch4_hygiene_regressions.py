"""Batch 4 regression tests for astrbot_plugin_group_chronicle.

Covers:
- `_conf_schema.json` prerequisite hints on options that are inert under default
  configuration (`history_enabled=true`, `persona_mode=off`, `enable_weekly_push=false`,
  `render_mode=t2i`).
- `tools/package_release.py` single top-level directory install ZIP hygiene &
  dev-file blacklist (`.github/`, `tests/`, `tools/`, `package-lock.json`,
  `build_release.py`, `.gitignore`).
"""

from __future__ import annotations

import json
import pathlib
import zipfile

from tools import package_release

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_batch4_schema_prereq_hints_on_default_inert_options():
    schema = json.loads((PLUGIN_ROOT / "_conf_schema.json").read_text(encoding="utf-8"))

    # Options that only take effect in compat mode (history_enabled=false) or specific commands
    compat_mode_keys = [
        "biography_days",
        "max_msgs",
        "max_memories",
        "influence_w_lm",
        "influence_w_sl",
        "influence_w_ra",
        "min_activity_messages",
        "enable_two_phase",
        "enable_golden_quotes",
        "enable_relation_arc",
        "enable_structured",
        "enable_gallery",
        "enable_slang",
    ]
    for key in compat_mode_keys:
        hint = str(schema[key].get("hint") or "")
        assert "前置条件" in hint and "history_enabled=false" in hint, (
            f"{key} missing explicit history_enabled=false prerequisite hint: {hint!r}"
        )

    # Options gated by persona_mode (default off)
    assert "前置条件" in schema["persona_id"]["hint"]
    assert "core_persona" in schema["persona_id"]["hint"]
    assert "前置条件" in schema["custom_persona_prompt"]["hint"]
    assert "custom_prompt" in schema["custom_persona_prompt"]["hint"]

    # Options gated by enable_weekly_push (default false)
    for key in ("push_weekday", "push_time", "push_groups", "enable_cron_reload"):
        hint = str(schema[key].get("hint") or "")
        assert "前置条件" in hint and "enable_weekly_push=true" in hint, (
            f"{key} missing enable_weekly_push=true prerequisite hint: {hint!r}"
        )

    # Options gated by render_mode=t2i
    for key in ("render_fallback_playwright", "t2i_scale"):
        hint = str(schema[key].get("hint") or "")
        assert "前置条件" in hint and "render_mode=t2i" in hint, (
            f"{key} missing render_mode=t2i prerequisite hint: {hint!r}"
        )


def test_batch4_chronicle_package_release_single_root_and_dev_blacklist(
    tmp_path: pathlib.Path,
):
    records = package_release.build(PLUGIN_ROOT, tmp_path / "dist")
    install_rec = next(r for r in records if r["kind"] == "install")
    zip_path = tmp_path / "dist" / install_rec["file"]
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    assert names
    top_dirs = {n.split("/", 1)[0] for n in names}
    assert top_dirs == {"astrbot_plugin_group_chronicle"}
    forbidden_dirs = {".github", "tests", "tools", "docs", "scripts", "frontend"}
    forbidden_files = {
        "package.json",
        "package-lock.json",
        "build_release.py",
        ".gitignore",
        ".gitattributes",
        "pyproject.toml",
        "pytest.ini",
    }
    for name in names:
        rel_parts = name.split("/")[1:]
        assert not (set(rel_parts) & forbidden_dirs), f"Dev dir leaked: {name}"
        assert rel_parts[-1] not in forbidden_files, f"Dev file leaked: {name}"
