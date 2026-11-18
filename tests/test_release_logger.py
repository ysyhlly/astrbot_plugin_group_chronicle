"""Exercise the same packager used for release archives."""

from pathlib import Path
from runpy import run_path

import pytest


@pytest.mark.parametrize(("relative", "statement"), [
    ("core/worker.py", "import logging as log"),
    ("nested/bridge.py", "from logging import getLogger"),
    ("engines/nested/worker.py", "import logging.handlers"),
])
def test_release_rejects_non_sdk_logging(tmp_path, relative, statement):
    build = run_path(str(Path(__file__).parents[1] / "tools" / "package_release.py"))["build"]
    root = tmp_path / "plugin"
    target = root / relative
    target.parent.mkdir(parents=True)
    (root / "metadata.yaml").write_text("name: audit_fixture\nversion: 1.0\n", encoding="utf-8")
    for name in ("main.py", "CHANGELOG.md", "LICENSE", "RELEASE_NOTES.md"):
        (root / name).write_text("", encoding="utf-8")
    target.write_text(statement, encoding="utf-8")
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="runtime must use astrbot.api.logger"):
        build(root, output)
    assert not list(output.glob("*-install.zip"))
    target.write_text("from astrbot.api import logger\nlogger.debug('ready')", encoding="utf-8")
    assert len(build(root, output)) == 2
