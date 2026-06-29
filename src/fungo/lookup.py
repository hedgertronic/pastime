"""Chadwick Bureau player ID cross-reference lookup.

Maps MLBAM <-> FanGraphs <-> Baseball-Reference <-> Retrosheet IDs. Source:
https://github.com/chadwickbureau/register (split into 16 shards by the last hex
char of ``key_person``).

The ~6 MB register is fetched on demand via :func:`pastime.http.request_bytes`
and cached under the stdlib user cache dir
(``$XDG_CACHE_HOME``/``~/.cache`` -> ``pastime/chadwick_people.csv``). Call
:func:`refresh` or pass ``force_refresh=True`` to update it.
"""

from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import Any

from pastime import http

CHADWICK_SHARD_URL = "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people-{}.csv"
CHADWICK_SHARDS = "0123456789abcdef"

CACHE_DIR = (
    Path(os.environ.get("XDG_CACHE_HOME") or "~/.cache").expanduser() / "pastime"
)
CACHE_FILE = CACHE_DIR / "chadwick_people.csv"

# In-memory cache — populated on first lookup.
_ROWS: list[dict[str, Any]] | None = None


#####################################################################
# Download / cache
#####################################################################


def _fetch_all_shards() -> str:
    """Download all 16 Chadwick shards and concatenate them into one CSV blob.

    Returns:
        The combined CSV text (single header row followed by all data rows).
    """
    header: str | None = None
    out_lines: list[str] = []
    for shard in CHADWICK_SHARDS:
        url = CHADWICK_SHARD_URL.format(shard)
        text = http.request_bytes(url, timeout=60).decode("utf-8")
        lines = text.splitlines()
        if not lines:
            continue
        if header is None:
            header = lines[0]
            out_lines.append(header)
        out_lines.extend(lines[1:])
        time.sleep(0.2)  # be polite to GitHub
    return "\n".join(out_lines) + "\n"


def refresh() -> Path:
    """Re-download the Chadwick register, overwriting the on-disk cache.

    Returns:
        Path to the written cache file.
    """
    global _ROWS
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    text = _fetch_all_shards()
    CACHE_FILE.write_text(text, encoding="utf-8")
    _ROWS = None
    return CACHE_FILE


def _load(force_refresh: bool = False) -> list[dict[str, Any]]:
    """Load the register into memory, downloading/refreshing the cache if needed.

    Args:
        force_refresh: Re-download before loading.

    Returns:
        The full list of register rows.
    """
    global _ROWS
    if _ROWS is not None and not force_refresh:
        return _ROWS
    if force_refresh or not CACHE_FILE.exists():
        refresh()
    with open(CACHE_FILE, encoding="utf-8") as f:
        _ROWS = list(csv.DictReader(f))
    return _ROWS


#####################################################################
# Lookup
#####################################################################


def lookup(
    name: str | None = None,
    mlbam: int | str | None = None,
    fangraphs: int | str | None = None,
    bbref: str | None = None,
    bbref_minors: str | None = None,
    retro: str | None = None,
    mlb_only: bool = True,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Look up players by any identifier or by name substring.

    Args:
        name: Full / first / last name substring (case-insensitive).
        mlbam: MLBAM (Savant) player ID.
        fangraphs: FanGraphs player ID.
        bbref: Baseball-Reference major-league ID (e.g. ``"troutmi01"``).
        bbref_minors: Baseball-Reference minor-league ID.
        retro: Retrosheet player ID.
        mlb_only: If True, restrict to players with any MLB seasons.
        force_refresh: Re-download the register before looking up.

    Returns:
        Matching rows. Keys include ``key_mlbam``, ``key_fangraphs``,
        ``key_bbref``, ``key_bbref_minors``, ``key_retro``, ``name_first``,
        ``name_last``, ``name_given``, ``mlb_played_first``, ``mlb_played_last``.
    """
    rows = _load(force_refresh=force_refresh)
    name_lc = name.lower() if name else None

    out = []
    for r in rows:
        if mlbam is not None and r.get("key_mlbam") != str(mlbam):
            continue
        if fangraphs is not None and r.get("key_fangraphs") != str(fangraphs):
            continue
        if bbref is not None and r.get("key_bbref") != str(bbref):
            continue
        if bbref_minors is not None and r.get("key_bbref_minors") != str(bbref_minors):
            continue
        if retro is not None and r.get("key_retro") != str(retro):
            continue
        if name_lc is not None:
            fn = (r.get("name_first") or "").lower()
            ln = (r.get("name_last") or "").lower()
            full = f"{fn} {ln}".strip()
            if name_lc not in fn and name_lc not in ln and name_lc not in full:
                continue
        if mlb_only and not (r.get("mlb_played_first") or r.get("mlb_played_last")):
            continue
        out.append(r)
    return out


#####################################################################
# ID converters
#####################################################################


def mlbam_to_fangraphs(mlbam: int | str) -> str | None:
    """Convert an MLBAM ID to a FanGraphs ID, or ``None`` if not found."""
    matches = lookup(mlbam=mlbam, mlb_only=False)
    if not matches:
        return None
    return matches[0].get("key_fangraphs") or None


def mlbam_to_bbref(mlbam: int | str) -> str | None:
    """Convert an MLBAM ID to a Baseball-Reference ID, or ``None`` if not found."""
    matches = lookup(mlbam=mlbam, mlb_only=False)
    if not matches:
        return None
    return matches[0].get("key_bbref") or None


def fangraphs_to_mlbam(fangraphs: int | str) -> str | None:
    """Convert a FanGraphs ID to an MLBAM ID, or ``None`` if not found."""
    matches = lookup(fangraphs=fangraphs, mlb_only=False)
    if not matches:
        return None
    return matches[0].get("key_mlbam") or None


def bbref_to_mlbam(bbref: str) -> str | None:
    """Convert a Baseball-Reference ID to an MLBAM ID, or ``None`` if not found."""
    matches = lookup(bbref=bbref, mlb_only=False)
    if not matches:
        return None
    return matches[0].get("key_mlbam") or None
