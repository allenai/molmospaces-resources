import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import tyro

from molmospaces_resources import HFRemoteStorage, R2RemoteStorage, ResourceManager
from molmospaces_resources.behaviors import InstallMode, LinkStrategy, SourceBehavior

logger = logging.getLogger("molmospaces_resources")
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())

TYPE_TO_URL: dict[str, str] = {
    "mjcf": "https://pub-3555e9bb2d304fab9c6c79819e48aa40.r2.dev",
    "usd": "https://pub-96496c3574b24d0c98b235219711d359.r2.dev",
}

TYPE_TO_BUCKET: dict[str, str] = {
    "mjcf": "mujoco-thor-resources",
    "usd": "isaac-thor-resources",
}

TYPE_TO_PREFIX: dict[str, str] = {
    "mjcf": "mujoco",
    "usd": "isaac",
}


@dataclass
class DownloadArgs:
    # Path to the asset manifest a json file that will override source to version and version flag
    asset_manifest: Path

    # `mjcf` for MuJoCo or ManiSkill, `usd` for Isaac
    type: Literal["mjcf", "usd"]

    # Path to symlink extracted data from the cache_dir
    install_dir: Path = Path("./assets")

    assets: list[Literal["thor", "objaverse"]] = field(default_factory=list)

    robots: list[str] = field(default_factory=list)

    scenes: list[
        Literal[
            "ithor",
            "procthor-10k-train",
            "procthor-10k-val",
            "procthor-10k-test",
            "procthor-objaverse-train",
            "procthor-objaverse-val",
            "holodeck-objaverse-train",
            "holodeck-objaverse-val",
        ]
    ] = field(default_factory=list)

    # Path to extract (versioned) downloaded data
    cache_dir: Path | None = None

    # Override VERSION in this scrip's resource tree
    version: str | None = None

    # If not provided, uses HF_TOKEN from environment
    hf_token: str | None = None

    # Use R2 remote storage (HuggingFace by default)
    use_r2: bool = True

    # When you want to download a version but not replace your symlink to it, pass True
    skip_symlink: bool = False


def main() -> int:
    args = tyro.cli(DownloadArgs)

    args.install_dir.mkdir(parents=True, exist_ok=True)

    assert args.type in TYPE_TO_PREFIX and args.type in TYPE_TO_URL, (
        f"Something went wrong, must only use {set(TYPE_TO_PREFIX.keys())}, but got '{args.type}'"
    )

    logger.info(f"Symlinking from directory '{args.install_dir}'")
    logger.info(f"Downloading '{args.type}' version of the assets")

    sources_to_version = dict(objects=dict(), robots=dict(), scenes=dict())

    try:
        with open(args.asset_manifest, "r") as f:
            manifest_object = json.load(f)
            for data_type, source_map in manifest_object.items():
                sources_to_version[data_type] = {
                    source: version for (source, version) in source_map.items()
                }
    except FileNotFoundError:
        logger.warning(
            f"Manifest file '{args.asset_manifest}' not found, make sure it's in path provided or use absolute path."
        )
    except Exception:
        sample_manifest = {"resource_type": {"source": {"version_string"}}}
        logger.error(
            f"Invalid manifest file '{args.asset_manifest}' make sure the structure is: {sample_manifest}"
        )
        return 1

    print(sources_to_version)

    cache_dir = args.cache_dir or Path.home() / f".molmospaces-{args.type}"

    manager = ResourceManager(
        remote_storage=R2RemoteStorage(TYPE_TO_BUCKET[args.type])
        if args.use_r2
        else HFRemoteStorage(
            repo_id="allenai/molmospaces",
            repo_prefix=TYPE_TO_PREFIX[args.type],
            token=args.hf_token or os.getenv("HF_TOKEN"),
        ),
        data_type_to_source_to_version=sources_to_version,
        symlink_dir=args.install_dir,
        cache_dir=cache_dir,
        data_type_defaults={
            "robots": SourceBehavior(LinkStrategy.PER_FILE, InstallMode.EAGER),
            "objects": SourceBehavior(LinkStrategy.GLOBAL, InstallMode.EAGER),
            "scenes": SourceBehavior(LinkStrategy.GLOBAL, InstallMode.EAGER),
        },
        force_install=True,
    )
    manager.setup()

    for data_type in sources_to_version:
        logger.info(f"Installing {data_type}...")
        manager.install_all_for_data_type(data_type, skip_linking=args.skip_symlink)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
