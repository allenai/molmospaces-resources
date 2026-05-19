import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
import sys

import tyro
from molmospaces_resources import HFRemoteStorage, R2RemoteStorage, ResourceManager
from molmospaces_resources.behaviors import (
    LinkStrategy,
    InstallMode,
    SourceBehavior
)

logger = logging.getLogger("molmospaces_resources")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

DEFAULT_CACHE_DIR = Path.home() / ".molmospaces"
VERSION = "20260518"
SOURCE_TO_VERSION = {
    "objects": {
       "train_blocks": {
           "usd": VERSION,
           "mjcf": VERSION
        },
       "train_blocks_legacy_match": {
           "usd": VERSION,
           "mjcf": VERSION
        },
       "robot_workstation": {
           "usd": VERSION,
           "mjcf": VERSION
        },
    },
    "robots": {
        "franka_droid": {
           "usd": VERSION,
           "mjcf": VERSION
        }
    }
}

TYPE_TO_URL: dict[str, str] = {
    "mjcf": "https://pub-68edf05dda9641c199fbab7951b156b4.r2.dev",
    "usd": "https://pub-7509f9a77f9742c8b936076bfc11ef68.r2.dev",
}

TYPE_TO_PREFIX: dict[str, str] = {
    "mjcf": "mujoco",
    "usd": "isaac",
}


@dataclass
class DownloadArgs:
    # `mjcf` for MuJoCo or ManiSkill, `usd` for Isaac
    type: Literal["mjcf", "usd"]

    # Path to symlink extracted data from the cache_dir
    install_dir: Path

    assets: list[Literal["train_blocks", "train_blocks_legacy_match", "robot_workstation"]] = field(default_factory=list)

    robots: list[str] = field(default_factory=list)

    # Path to extract (versioned) downloaded data
    cache_dir: Path = DEFAULT_CACHE_DIR

    # If not provided, uses HF_TOKEN from environment
    hf_token: str | None = None

    # Use R2 remote storage (HuggingFace by default)
    use_r2: bool = False

    # Override VERSION 
    version: str = None

def main() -> int:
    args = tyro.cli(DownloadArgs)

    args.install_dir.mkdir(parents=True, exist_ok=True)

    assert args.type in TYPE_TO_PREFIX and args.type in TYPE_TO_URL, (
        f"Something went wrong, must only use {set(TYPE_TO_PREFIX.keys())}, but got '{args.type}'"
    )

    logger.info(f"Symlinking from directory '{args.install_dir}'")
    logger.info(f"Downloading '{args.type}' version of the assets")

    sources_to_version = dict(objects=dict(), robots=dict())

    for (data_type, source_map) in SOURCE_TO_VERSION.items():
        sources_to_version[data_type] = { f"{source}/{args.type}":(args.version if args.version else type_map[args.type]) for (source, type_map) in source_map.items()}

    print(sources_to_version)

    manager = ResourceManager(
        remote_storage=R2RemoteStorage(f"toyblocks-resources")
        if args.use_r2
        else HFRemoteStorage(
            repo_id="allenai/molmospaces",
            repo_prefix=TYPE_TO_PREFIX[args.type],
            token=args.hf_token or os.getenv("HF_TOKEN"),
        ),
        data_type_to_source_to_version=sources_to_version,
        symlink_dir=args.install_dir,
        cache_dir=args.cache_dir,
        data_type_defaults={
            "robots": SourceBehavior(LinkStrategy.PER_FILE, InstallMode.EAGER),
            "objects": SourceBehavior(
                LinkStrategy.GLOBAL, InstallMode.EAGER
            ),
        },
        force_install=True,
    )
    manager.setup()

    logger.info("Installing objects...")
    manager.install_all_for_data_type("objects", skip_linking=False)

    logger.info("Installing robots...")
    manager.install_all_for_data_type("robots", skip_linking=False)

    return 0

def usd_default():
    sys.argv = [sys.argv[0], "--use-r2", "--type", "usd", "--install-dir", "assets", *sys.argv[1:]]
    main()

def mjcf_default():
    sys.argv = [sys.argv[0], "--use-r2", "--type", "mjcf", "--install-dir", "assets", *sys.argv[1:]]
    main()

if __name__ == "__main__":
    raise SystemExit(main())
