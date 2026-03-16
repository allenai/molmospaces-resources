from __future__ import annotations

import logging
import tarfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from typing import TYPE_CHECKING

import zstandard as zstd
from tqdm import tqdm

from molmospaces_resources.file_utils import _safe_extract, _complete_extract_flag
from molmospaces_resources.constants import TQDM_DISABLE_THRES

if TYPE_CHECKING:
    from molmospaces_resources.remote_storage import RemoteStorage

logger = logging.getLogger("molmospaces_resources")


def _download_and_extract(
    package: str,
    relative_path: Path,
    cache_dest: Path,
    source: RemoteStorage,
    *,
    read_only: bool = True,
) -> bool:
    """Download a single ``.tar.zst`` archive and extract it to *cache_dest*."""
    try:
        if not package.endswith(".tar.zst"):
            raise RuntimeError(f"Unknown archive extension: {package}")
        raw_stream = source.stream_archive(relative_path, package)
        with zstd.ZstdDecompressor().stream_reader(raw_stream) as reader:
            with tarfile.open(fileobj=reader, mode="r|*") as tar:
                _safe_extract(tar, cache_dest, read_only=read_only)
        _complete_extract_flag(package, cache_dest).touch(exist_ok=False)
        return True
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        logger.warning("Download/extract failure for %s: %s: %s", package, type(exc).__name__, exc)
        return False


def _extract_worker(q_in: Queue, q_out: Queue) -> None:
    try:
        while True:
            item = q_in.get()
            if item is None:
                break
            package, relative_path, cache_dest, source, read_only = item
            ok = _download_and_extract(
                package, relative_path, cache_dest, source, read_only=read_only
            )
            q_out.put((package, ok))
    finally:
        q_out.put(None)


def _logging_worker(
    q_out: Queue,
    manifest: dict[str, float],
    num_workers: int,
    result: dict,
) -> None:
    pending = set(manifest)
    failed: set[str] = set()
    ended = 0
    pbar = tqdm(
        total=len(manifest),
        desc="Extracting",
        disable=len(manifest) < TQDM_DISABLE_THRES,
    )
    try:
        while True:
            msg = q_out.get()
            if msg is None:
                ended += 1
                if ended == num_workers:
                    break
                continue
            name, ok = msg
            pending.discard(name)
            if not ok:
                failed.add(name)
            pbar.update(1)
    finally:
        pbar.close()
    result["failed"] = pending | failed


def _parallel_extract(
    packages_to_size: dict[str, float],
    relative_path: Path,
    cache_dest: Path,
    source: RemoteStorage,
    *,
    read_only: bool = True,
    max_workers: int = 10,
    min_items_per_worker: int = 10,
) -> set[str]:
    """Download + extract *packages_to_size* in parallel.  Returns failed package names."""
    if not packages_to_size:
        return set()

    q_in: Queue = Queue()
    q_out: Queue = Queue()
    for pkg in packages_to_size:
        q_in.put((pkg, relative_path, cache_dest, source, read_only))

    num_workers = max(min(max_workers, len(packages_to_size) // min_items_per_worker), 1)
    for _ in range(num_workers):
        q_in.put(None)  # sentinel

    if len(packages_to_size) > 1:
        logger.debug("Downloading %d packages to cache...", len(packages_to_size))

    result: dict = {}
    with ThreadPoolExecutor(max_workers=num_workers + 1) as pool:
        pool.submit(_logging_worker, q_out, packages_to_size, num_workers, result)
        for _ in range(num_workers):
            pool.submit(_extract_worker, q_in, q_out)

    failed = result["failed"]
    if not failed or len(failed) > max(0.1 * len(packages_to_size), 1):
        return failed

    logger.debug("Retrying %d failures in main thread...", len(failed))
    retry_q_in: Queue = Queue()
    retry_q_out: Queue = Queue()
    for pkg in failed:
        retry_q_in.put((pkg, relative_path, cache_dest, source, read_only))
    retry_q_in.put(None)
    _extract_worker(retry_q_in, retry_q_out)
    retry_result: dict = {}
    retry_manifest = {p: packages_to_size[p] for p in failed}
    _logging_worker(retry_q_out, retry_manifest, 1, retry_result)
    return retry_result["failed"]
