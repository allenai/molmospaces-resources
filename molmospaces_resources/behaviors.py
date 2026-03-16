from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Any

from molmospaces_resources.indexing import ArchiveIndex, NumericIndex, SubstringIndex


class LinkStrategy(Enum):
    """How a source is exposed under ``symlink_dir``.

    **Why two strategies?**

    Scene XML files contain relative paths to sibling directories
    (e.g. ``../objects/thor/Bowl.xml``).  MuJoCo's XML loader resolves
    these relative to the *physical* file location, following symlinks.
    If scenes used a ``GLOBAL`` directory symlink, the physical location
    would be deep inside ``cache_dir``, and ``../objects/`` would resolve
    to a nonexistent cache path instead of ``symlink_dir/objects/``.

    ``PER_FILE`` avoids this: each scene file is an individual symlink
    *inside* a real directory under ``symlink_dir``, so relative paths
    resolve within the symlink tree where sibling data types are visible.

    Data types that are leaf data (objects, grasps, etc.) don't reference
    siblings via relative paths, so a single ``GLOBAL`` directory symlink
    is safe and simpler.
    """

    PER_FILE = "per_file"
    """Real directory with individual file symlinks into the cache."""

    GLOBAL = "global"
    """Single directory-level symlink pointing at the versioned cache dir."""


class InstallMode(Enum):
    """When archive contents are extracted."""

    EAGER = "eager"
    """Extract all archives at setup time."""

    ON_DEMAND = "on_demand"
    """Only fetch manifests at setup; extract archives on first use."""


@dataclass
class SourceBehavior:
    """Per-source behaviour configuration.

    ``archive_index`` may be a *class* (instantiated per source in
    ``_resolve_behavior``) or an *instance* (used as-is).  Storing the
    class in ``DATA_TYPE_DEFAULTS`` ensures each source gets its own
    index instance with independent ``build()`` state.
    """

    link_strategy: LinkStrategy
    install_mode: InstallMode
    archive_index: type | ArchiveIndex | None = None


DATA_TYPE_DEFAULTS: dict[str, SourceBehavior] = {
    "robots": SourceBehavior(LinkStrategy.PER_FILE, InstallMode.EAGER),
    "scenes": SourceBehavior(
        LinkStrategy.PER_FILE, InstallMode.ON_DEMAND, NumericIndex
    ),
    "objects": SourceBehavior(
        LinkStrategy.GLOBAL, InstallMode.ON_DEMAND, SubstringIndex
    ),
    "grasps": SourceBehavior(
        LinkStrategy.GLOBAL, InstallMode.ON_DEMAND, SubstringIndex
    ),
    "test_data": SourceBehavior(LinkStrategy.GLOBAL, InstallMode.EAGER),
    "benchmarks": SourceBehavior(LinkStrategy.GLOBAL, InstallMode.EAGER),
    "franka-rby1-training-data": SourceBehavior(
        LinkStrategy.GLOBAL, InstallMode.ON_DEMAND, NumericIndex
    ),
}

SOURCE_OVERRIDES: dict[tuple[str, str], dict[str, Any]] = {
    ("objects", "thor"): {"install_mode": InstallMode.EAGER},
    ("objects", "objathor_metadata"): {"install_mode": InstallMode.EAGER},
    ("grasps", "droid"): {"install_mode": InstallMode.EAGER},
    ("grasps", "rum"): {"install_mode": InstallMode.EAGER},
}


def _resolve_behavior(
    data_type: str,
    source: str,
    defaults: dict[str, SourceBehavior],
    overrides: dict[tuple[str, str], dict[str, Any]],
) -> SourceBehavior:
    """Return the effective ``SourceBehavior`` for *(data_type, source)*.

    If ``archive_index`` is a class (type), it is instantiated so each
    source gets its own index with independent ``build()`` state.
    Instantiation happens *after* applying overrides, so an override can
    replace the index class/instance.
    """
    base = defaults.get(data_type)
    if base is None:
        raise ValueError(
            f"No default SourceBehavior for data_type={data_type!r}. "
            f"Known types: {sorted(defaults)}"
        )
    patch = overrides.get((data_type, source))
    result = dataclasses.replace(base, **patch) if patch else base
    # Instantiate if archive_index is a class rather than an instance
    if isinstance(result.archive_index, type):
        result = dataclasses.replace(result, archive_index=result.archive_index())
    return result
