"""Pluggable data sources for fetching archives and metadata.

Two built-in implementations:

* :class:`HFRemoteStorage` — fetches files from a Hugging Face dataset repo,
  using Range requests into pre-built tar shards indexed via HF Datasets.
* :class:`R2RemoteStorage` — fetches files from an R2/HTTP public bucket URL (legacy).
"""

from __future__ import annotations

import io
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)


class RemoteStorage(ABC):
    """Interface for fetching remote files and streaming archives."""

    @abstractmethod
    def fetch_file(self, relative_path: Path, filename: str, dest: Path) -> None:
        """Download *filename* under *relative_path* and write it to *dest/filename*."""
        ...

    @abstractmethod
    def stream_archive(self, relative_path: Path, archive_name: str) -> io.RawIOBase:
        """Return a readable binary stream for the raw (compressed) archive bytes."""
        ...


class R2RemoteStorage(RemoteStorage):
    """Fetch from an R2/HTTP public bucket URL (legacy).

    URL pattern: ``base_url/relative_path/filename``.
    """

    KNOWN_URLS: dict[str, str] = {
        "mujoco-thor-resources": "https://pub-3555e9bb2d304fab9c6c79819e48aa40.r2.dev",
        "isaac-thor-resources": "https://pub-96496c3574b24d0c98b235219711d359.r2.dev",
        "mujoco-thor-training-data": "https://pub-8835d6d5740d4643905321ed0a7f6ecb.r2.dev",
    }

    def __init__(self, base_url: str) -> None:
        if not base_url.startswith("http"):
            if base_url not in self.KNOWN_URLS:
                raise ValueError(
                    f"Unknown bucket {base_url!r}. "
                    f"Provide a full URL or one of {sorted(self.KNOWN_URLS)}."
                )
            base_url = self.KNOWN_URLS[base_url]
        self.base_url = base_url

    def fetch_file(self, relative_path: Path, filename: str, dest: Path) -> None:
        url = f"{self.base_url}/{relative_path.as_posix()}/{filename}"
        logger.debug("Downloading %s/%s...", relative_path, filename)
        resp = requests.get(url)
        resp.raise_for_status()
        with open(dest / filename, "wb") as f:
            f.write(resp.content)

    def stream_archive(self, relative_path: Path, archive_name: str) -> io.RawIOBase:
        url = f"{self.base_url}/{relative_path.as_posix()}/{archive_name}"
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        return resp.raw

    def __repr__(self) -> str:
        return f"R2RemoteStorage({self.base_url!r})"


class HFRemoteStorage(RemoteStorage):
    """Fetch archives from a Hugging Face dataset repo using Range requests.

    Requires the mirroring script to have uploaded tar shards and a
    Datasets-format index (``config_name=<__separated_path>``, ``split="pkgs"``)
    mapping archive names to shard offsets.

    Parameters
    ----------
    repo_id:
        HF dataset repo (e.g. ``"allenai/molmospaces"``).
    repo_prefix:
        Top-level directory inside the repo that mirrors the R2 bucket
        (e.g. ``"mujoco"`` for the mujoco-thor-resources bucket).
    revision:
        Branch or commit to read from.
    token:
        Optional HF token for private repos.
    """

    def __init__(
        self,
        repo_id: str,
        repo_prefix: str,
        *,
        revision: str = "main",
        token: str | None = None,
    ) -> None:
        self.repo_id = repo_id
        self.repo_prefix = repo_prefix
        self.revision = revision
        self.token = token
        self._index_cache: dict[str, dict[str, Any]] = {}

    def _hf_dir(self, relative_path: Path) -> str:
        """Map a relative_path (data_type/source/version) to the HF directory."""
        return f"{self.repo_prefix}/{relative_path.as_posix()}"

    def _config_name(self, relative_path: Path) -> str:
        """Derive the HF Datasets config name (``__``-separated path)."""
        return self._hf_dir(relative_path).replace("/", "__")

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _file_url(self, path_in_repo: str) -> str:
        return (
            f"https://huggingface.co/datasets/{self.repo_id}"
            f"/resolve/{self.revision}/{path_in_repo}"
        )

    def _load_index(self, relative_path: Path) -> dict[str, Any]:
        """Load (and disk-cache) the shard index for *relative_path*.

        The mirror script uploads each index as a HF Dataset with
        ``config_name=hf_dir.replace("/", "__")`` and ``split="pkgs"``.
        Using :func:`datasets.load_dataset` gives us free persistent
        caching in ``~/.cache/huggingface/datasets/``.
        """
        from datasets import load_dataset

        key = relative_path.as_posix()
        if key not in self._index_cache:
            config = self._config_name(relative_path)
            logger.debug(
                "Loading HF shard index: repo=%s config=%s", self.repo_id, config
            )
            ds = load_dataset(
                self.repo_id,
                name=config,
                split="pkgs",
                revision=self.revision,
                token=self.token,
            )
            self._index_cache[key] = {row["path"]: row for row in ds}
        return self._index_cache[key]

    def fetch_file(self, relative_path: Path, filename: str, dest: Path) -> None:
        hf_dir = self._hf_dir(relative_path)
        url = self._file_url(f"{hf_dir}/{filename}")
        logger.debug("Downloading %s/%s from HF...", relative_path, filename)
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        with open(dest / filename, "wb") as f:
            f.write(resp.content)

    def stream_archive(self, relative_path: Path, archive_name: str) -> io.RawIOBase:
        index = self._load_index(relative_path)
        if archive_name not in index:
            raise ValueError(
                f"Archive {archive_name!r} not found in HF shard index "
                f"for {relative_path.as_posix()}"
            )

        entry = index[archive_name]
        hf_dir = self._hf_dir(relative_path)
        shard_file = f"{hf_dir}/shards/{entry['shard_id']:05d}.tar"
        url = self._file_url(shard_file)

        start = entry["offset"]
        end = start + entry["size"] - 1
        headers = self._headers()
        headers["Range"] = f"bytes={start}-{end}"

        logger.debug(
            "Fetching %s from HF (shard %d, offset %d, size %d)",
            archive_name,
            entry["shard_id"],
            start,
            entry["size"],
        )
        resp = requests.get(url, headers=headers, stream=True)
        resp.raise_for_status()
        return resp.raw

    def __repr__(self) -> str:
        return f"HFRemoteStorage({self.repo_id!r}, prefix={self.repo_prefix!r})"
