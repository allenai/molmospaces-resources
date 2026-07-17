import logging
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Literal
import sys

import tyro
from molmospaces_resources import (
    R2RemoteStorage,
    GCRemoteStorage,
    ResourceManager,
)
from molmospaces_resources.behaviors import LinkStrategy, InstallMode, SourceBehavior

logger = logging.getLogger("molmospaces_resources")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

DEFAULT_CACHE_DIR = Path.home() / ".toyblocks"
VERSION = "20260518"
SOURCE_TO_VERSION = {
    "objects": {
        "train_blocks": {"usd": VERSION, "mjcf": VERSION},
        "train_blocks_legacy_match": {"usd": VERSION, "mjcf": VERSION},
        "robot_workstation": {"usd": VERSION, "mjcf": VERSION},
    },
    "robots": {"franka_droid": {"usd": VERSION, "mjcf": VERSION}},
    "scenes": {"toyblocks_real": {"usd": "20260521"}},
}

STORAGE_TO_TYPE_TO_URL: dict[str, dict[str, str]] = {
    "r2": {
        "mjcf": "https://pub-68edf05dda9641c199fbab7951b156b4.r2.dev",
        "usd": "https://pub-7509f9a77f9742c8b936076bfc11ef68.r2.dev",
    },
    "gc": {
        "mjcf": "https://storage.googleapis.com/toyblocks-resources-mjcf",
        "usd": "https://storage.googleapis.com/toyblocks-resources-usd",
    },
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
    install_dir: Path = Path("./assets")

    assets: list[
        Literal["train_blocks", "train_blocks_legacy_match", "robot_workstation"]
    ] = field(default_factory=list)

    robots: list[str] = field(default_factory=list)

    # Path to extract (versioned) downloaded data
    cache_dir: Path = DEFAULT_CACHE_DIR

    # Override VERSION in this scrip's resource tree
    version: str | None = None

    # Path to the asset manifest a json file that will override source to version and version flag
    asset_manifest: str | None = None

    # If not provided, uses HF_TOKEN from environment
    hf_token: str | None = None

    # Storage to use (Google Cloud by default)
    storage: list[Literal["r2", "gc"]] = field(default_factory=lambda: "gc")

    # When you want to download a version but not replace your symlink to it, pass True
    skip_symlink: bool = False


def main() -> int:
    args = tyro.cli(DownloadArgs)

    args.install_dir.mkdir(parents=True, exist_ok=True)

    assert (
        args.type in TYPE_TO_PREFIX
        and args.type in STORAGE_TO_TYPE_TO_URL[args.storage]
    ), (
        f"Something went wrong, must only use {set(TYPE_TO_PREFIX.keys())}, but got '{args.type}'"
    )

    logger.info(f"Symlinking from directory '{args.install_dir}'")
    logger.info(f"Downloading '{args.type}' version of the assets")

    sources_to_version = dict(objects=dict(), robots=dict(), scenes=dict())

    fallback_to_script_manifest = True
    if args.asset_manifest:
        try:
            with open(args.asset_manifest, "r") as f:
                manifest_object = json.load(f)
                for data_type, source_map in manifest_object.items():
                    sources_to_version[data_type] = {
                        f"{source}/{args.type}": version
                        for (source, version) in source_map.items()
                    }
                fallback_to_script_manifest = False
        except FileNotFoundError as e:
            logger.warning(
                f"Manifest file '{args.asset_manifest}' not found, make sure it's in path provided or use absolute path."
            )
        except Exception as e:
            sample_manifest = {"resource_type": {"source": {"version_string"}}}
            logger.error(
                f"Invalid manifest file '{args.asset_manifest}' make sure the structure is: {sample_manifest}"
            )
            exit(1)

    if not args.asset_manifest or fallback_to_script_manifest:
        for data_type, source_map in SOURCE_TO_VERSION.items():
            sources_to_version[data_type] = {
                f"{source}/{args.type}": (
                    args.version if args.version else type_map[args.type]
                )
                for (source, type_map) in source_map.items()
            }

    print(sources_to_version)

    if args.storage == "r2":
        remote_storage = R2RemoteStorage(f"toyblocks-resources")
    else:
        remote_storage = GCRemoteStorage(f"toyblocks-resources")

    manager = ResourceManager(
        remote_storage=remote_storage,
        data_type_to_source_to_version=sources_to_version,
        symlink_dir=args.install_dir,
        cache_dir=args.cache_dir,
        data_type_defaults={
            "robots": SourceBehavior(LinkStrategy.PER_FILE, InstallMode.EAGER),
            "objects": SourceBehavior(LinkStrategy.GLOBAL, InstallMode.EAGER),
            "scenes": SourceBehavior(LinkStrategy.GLOBAL, InstallMode.EAGER),
        },
        force_install=True,
    )
    manager.setup()

    for data_type, source_dict in sources_to_version.items():
        logger.info(f"Installing {data_type}...")
        manager.install_all_for_data_type(data_type, skip_linking=args.skip_symlink)

    return 0


def usd_default():
    sys.argv = [
        sys.argv[0],
        "--storage",
        "gc",
        "--type",
        "usd",
        "--install-dir",
        "assets",
        *sys.argv[1:],
    ]
    main()


def mjcf_default():
    sys.argv = [
        sys.argv[0],
        "--storage",
        "gc",
        "--type",
        "mjcf",
        "--install-dir",
        "assets",
        *sys.argv[1:],
    ]
    main()


if __name__ == "__main__":
    raise SystemExit(main())
