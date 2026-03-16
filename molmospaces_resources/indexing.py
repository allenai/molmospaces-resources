from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Sequence


class ArchiveIndex(ABC):
    """Maps a query string to *candidate* archive names (best-first).

    ``build()`` is called once with all known archive names for a source.
    ``candidates()`` returns a (possibly empty) sequence of archive names
    that are likely to contain the queried path.  The caller verifies
    against the trie; the index is just a fast-path hint.
    """

    @abstractmethod
    def build(self, archive_names: Sequence[str]) -> None: ...

    @abstractmethod
    def candidates(self, query: str, *, max_returns: int = 1) -> Sequence[str]:
        """Return candidate archives for *query*.

        *max_returns* controls filtering: if a single extracted token maps to
        more than *max_returns* archives, that token is considered too
        ambiguous and is skipped.
        """
        ...

    @abstractmethod
    def query_token(self, token: str) -> set[str]:
        """Return all archives matching a single index token (no filtering)."""
        ...

    @abstractmethod
    def unindexed(self, all_archive_names: Sequence[str]) -> list[str]:
        """Return archive names from *all_archive_names* that don't match any token."""
        ...


class NumericIndex(ArchiveIndex):
    """Index archives by integers found in their names.

    Useful for numbered datasets (e.g. ``ithor_FloorPlan301_400.tar.zst``).
    """

    _NUMBER_RE = re.compile(r"-?\d+")

    def __init__(self) -> None:
        self._number_to_archives: dict[str, set[str]] = {}

    def build(self, archive_names: Sequence[str]) -> None:
        self._number_to_archives.clear()
        for name in archive_names:
            for n in self._NUMBER_RE.findall(name):
                self._number_to_archives.setdefault(n, set()).add(name)

    def candidates(self, query: str, *, max_returns: int = 1) -> Sequence[str]:
        numbers = self._NUMBER_RE.findall(query)
        if not numbers:
            return ()
        hits: set[str] = set()
        for n in numbers:
            bucket = self._number_to_archives.get(n, set())
            if len(bucket) <= max_returns:
                hits |= bucket
        return tuple(hits)

    def query_token(self, token: str) -> set[str]:
        return set(self._number_to_archives.get(token, set()))

    def unindexed(self, all_archive_names: Sequence[str]) -> list[str]:
        indexed = set()
        for bucket in self._number_to_archives.values():
            indexed |= bucket
        return sorted(set(all_archive_names) - indexed)


class SubstringIndex(ArchiveIndex):
    """Index archives by delimiter-split tokens.

    Useful for named datasets (e.g. ``objaverse_abc123.tar.zst``).
    """

    SPLIT_RE = re.compile(r"[./_\s]+")

    def __init__(self) -> None:
        self._token_to_archives: dict[str, set[str]] = {}

    def build(self, archive_names: Sequence[str]) -> None:
        self._token_to_archives.clear()
        for name in archive_names:
            bare = name.replace(".tar.zst", "")
            for tok in self.SPLIT_RE.split(bare):
                if tok:
                    self._token_to_archives.setdefault(tok, set()).add(name)

    def candidates(self, query: str, *, max_returns: int = 1) -> Sequence[str]:
        tokens = [t for t in self.SPLIT_RE.split(query) if t]
        if not tokens:
            return ()
        hits: set[str] = set()
        for tok in tokens:
            bucket = self._token_to_archives.get(tok, set())
            if len(bucket) <= max_returns:
                hits |= bucket
        return tuple(hits)

    def query_token(self, token: str) -> set[str]:
        return set(self._token_to_archives.get(token, set()))

    def unindexed(self, all_archive_names: Sequence[str]) -> list[str]:
        indexed = set()
        for bucket in self._token_to_archives.values():
            indexed |= bucket
        return sorted(set(all_archive_names) - indexed)


def split_query_tokens(text: str) -> list[str]:
    """Split *text* into tokens by common path/name delimiters.

    Useful for substring-based archive lookups (same logic as
    ``SubstringIndex``).
    """
    return [t for t in SubstringIndex.SPLIT_RE.split(text) if t]
