"""fungo — tools for acquiring baseball data from Statcast, the MLB Stats API,
and the Chadwick player-ID register.

The core is stdlib-only and returns raw ``list[dict]`` / ``dict``. DataFrame
conversion lives in :mod:`fungo.frame` behind optional extras.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from fungo import constants, lookup, mlb, statcast
from fungo.exceptions import (
    FungoError,
    MLBStatsError,
    RequestError,
    SavantError,
    ValidationError,
)
from fungo.frame import to_frame

try:
    __version__ = version("fungo")
except PackageNotFoundError:  # pragma: no cover - source tree fallback
    __version__ = "1.0.0"

# Subpackages are the primary surface: `fungo.statcast.search_pitches(...)`,
# `fungo.mlb.get_schedule(...)`, `fungo.lookup.lookup(...)`. The names below
# are the small cross-cutting top level — exceptions, the optional DataFrame
# bridge, and the shared resolvers. Per-domain functions stay under their
# subpackage to avoid collisions (both statcast and mlb define PITCH_TYPES, etc.).
__all__ = [
    "FungoError",
    "MLBStatsError",
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
