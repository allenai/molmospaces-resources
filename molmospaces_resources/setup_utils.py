from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from pathlib import Path

from molmospaces_resources.constants import LOCAL_MANIFEST_NAME
from molmospaces_resources.remote_storage import RemoteStorage
from molmospaces_resources.manager import ResourceManager

logger = logging.getLogger(__name__)

_RESOURCE_MANAGERS: dict[str, ResourceManager] = {}
_RESOURCE_MANAGERS_PID: int = os.getpid()


def str2bool(v: str) -> bool:
    v = v.lower().strip()
    if v in ("yes", "true", "t", "y", "1"):
        return True
    if v in ("no", "false", "f", "n", "0"):
        return False
    raise ValueError(f"{v!r} cannot be converted to a bool")


def _env_bool(name: str, default: str = "True") -> bool:
    return str2bool(os.environ.get(name, default))


def _manager_key(remote_storage_str: str, versions: dict[str, dict[str, str]]) -> str:
    parts: list[str] = [remote_storage_str]
    for dt in sorted(versions):
        for src in sorted(versions[dt]):
            parts.append(f"{dt}/{src}/{versions[dt][src]}")
    return "|".join(parts)


def _get_current_install(
    assets_dir: Path,
    versions: dict[str, dict[str, str]],
) -> dict:
    current: dict = {}
    manifest_path = Path(assets_dir) / LOCAL_MANIFEST_NAME
    if manifest_path.is_file():
        with open(manifest_path) as f:
            current.update(json.load(f))
    for dt, sv in versions.items():
        current.setdefault(dt, {})
        for src in sv:
            current[dt].setdefault(src, None)
    return current


def _needs_install(
    current: dict,
    versions: dict[str, dict[str, str]],
) -> bool:
    return any(current[dt][src] != ver for dt, sv in versions.items() for src, ver in sv.items())


def setup_resource_manager(
    remote_storage: RemoteStorage,
    symlink_dir: str | Path,
    versions: dict[str, dict[str, str]],
    cache_dir: str | Path,
    *,
    env_prefix: str = "MLSPACES",
    force_install: bool | None = None,
    cache_lock: bool | None = None,
    post_setup: Callable[[ResourceManager], None] | None = None,
    force_post_setup: bool = False,
) -> ResourceManager:
    """Create (or retrieve a cached) :class:`ResourceManager` and run setup if needed.

    Parameters
    ----------
    remote_storage:
        A :class:`RemoteStorage` instance (e.g. :class:`HFRemoteStorage` or :class:`R2RemoteStorage`).
    symlink_dir:
        Symlink / install directory.
    versions:
        ``{data_type: {source: version}}`` mapping.
    cache_dir:
        Download cache directory (versioned).
    env_prefix:
        Prefix for environment variable overrides.  The following env vars
        are read (all default to ``"True"``):

        * ``{env_prefix}_FORCE_INSTALL`` — overwrite existing version mismatches.
        * ``{env_prefix}_CACHE_LOCK`` — acquire the cache lock.
    force_install:
        Override for ``{env_prefix}_FORCE_INSTALL``.
    cache_lock:
        Override for ``{env_prefix}_CACHE_LOCK``.
    post_setup:
        Optional callback invoked with the manager after ``setup()`` completes.
        By default only called when new sources are installed.
    force_post_setup:
        If ``True``, run *post_setup* even when all versions are already
        up to date.  Useful for eagerly installing on-demand packages
        that ``setup()`` intentionally skips.
    """
    if force_install is None:
        force_install = _env_bool(f"{env_prefix}_FORCE_INSTALL")
    if cache_lock is None:
        cache_lock = _env_bool(f"{env_prefix}_CACHE_LOCK")

    global _RESOURCE_MANAGERS_PID
    key = _manager_key(str(remote_storage), versions)

    if os.getpid() != _RESOURCE_MANAGERS_PID:
        _RESOURCE_MANAGERS.clear()
        _RESOURCE_MANAGERS_PID = os.getpid()

    if key in _RESOURCE_MANAGERS:
        return _RESOURCE_MANAGERS[key]

    manager = ResourceManager(
        remote_storage=remote_storage,
        data_type_to_source_to_version=versions,
        symlink_dir=Path(symlink_dir),
        cache_dir=Path(cache_dir),
        force_install=force_install,
        cache_lock=cache_lock,
    )
    _RESOURCE_MANAGERS[key] = manager

    current = _get_current_install(Path(symlink_dir), versions)

    if _needs_install(current, versions):
        logger.debug("Installing missing data versions...")
        manager.setup()

        if post_setup is not None:
            post_setup(manager)
    else:
        logger.debug("All data versions are up to date")
        if force_post_setup and post_setup is not None:
            post_setup(manager)

    return manager
