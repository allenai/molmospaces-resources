from __future__ import annotations

import contextlib
import json
import logging
import stat
import tarfile
from pathlib import Path

from filelock import FileLock

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def _lock_context(symlink_dir: Path | None, cache_dir: Path | None):
    """Acquire file locks on *symlink_dir*, *cache_dir*, both, or neither."""
    if symlink_dir is not None and cache_dir is not None:
        with FileLock(symlink_dir / ".lock"), FileLock(cache_dir / ".lock"):
            yield
    elif symlink_dir is not None:
        with FileLock(symlink_dir / ".lock"):
            yield
    elif cache_dir is not None:
        with FileLock(cache_dir / ".lock"):
            yield
    else:
        yield


def _safe_extract(tar: tarfile.TarFile, dest: Path, *, read_only: bool = True) -> None:
    """Extract *tar* into *dest*, guarding against path traversal."""
    resolved_root = dest.resolve()
    for member in tar:
        member_path = dest / member.name
        if not member_path.resolve().is_relative_to(resolved_root):
            raise RuntimeError(f"Path traversal in tar: {member.name}")
        try:
            tar.extract(member, path=dest)
        except PermissionError:
            if not member_path.exists():
                raise
        if read_only and member_path.is_file():
            member_path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def _complete_extract_flag(package: str, cache_dest: Path) -> Path:
    return cache_dest / f".{package.replace('/', '__')}_complete_extract"


def _complete_link_flag(package: str, link_dir: Path) -> Path:
    return link_dir / f".{package.replace('/', '__')}_complete_links"


def load_json_manifest(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def save_json_manifest(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
