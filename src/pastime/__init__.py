"""pastime — tools for acquiring baseball data from Statcast, the MLB Stats API,
and the Chadwick player-ID register.

The core is stdlib-only and returns raw ``list[dict]`` / ``dict``. DataFrame
conversion lives in :mod:`pastime.frame` behind optional extras.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from pastime import constants, lookup, mlb, statcast
from pastime.exceptions import (
    MLBStatsError,
    PastimeError,
    RequestError,
    SavantError,
    ValidationError,
)
from pastime.frame import to_frame

try:
    __version__ = version("pastime")
except PackageNotFoundError:  # pragma: no cover - source tree fallback
    __version__ = "1.0.0"

# Subpackages are the primary surface: `pastime.statcast.statcast_search(...)`,
# `pastime.mlb.get_schedule(...)`, `pastime.lookup.lookup(...)`. The names below
# are the small cross-cutting top level — exceptions, the optional DataFrame
# bridge, and the shared resolvers. Per-domain functions stay under their
# subpackage to avoid collisions (both statcast and mlb define PITCH_TYPES, etc.).
__all__ = [
    "MLBStatsError",
    "PastimeError",
    "RequestError",
    "SavantError",
    "ValidationError",
    "__version__",
    "constants",
    "lookup",
    "mlb",
    "statcast",
    "to_frame",
]
