"""Optional DataFrame conversion for fungo row data.

The core returns ``list[dict]`` with all-string CSV values. :func:`to_frame`
converts those rows into a polars or pandas DataFrame, importing the chosen
backend lazily so the core stays dependency-free. Install the matching extra
(``fungo[polars]`` or ``fungo[pandas]``) to use it.
"""

from __future__ import annotations

from typing import Any

from fungo.exceptions import ValidationError

#####################################################################
# DataFrame conversion
#####################################################################


def to_frame(rows: list[dict[str, Any]], backend: str = "polars") -> Any:
    """Convert ``list[dict]`` rows to a DataFrame using the chosen backend.

    The backend is imported lazily; a missing dependency raises a clear,
    actionable error rather than failing at import time.

    Args:
        rows: Row dicts (e.g. from a Statcast or lookup call).
        backend: ``"polars"`` (default) or ``"pandas"``.

    Returns:
        A ``polars.DataFrame`` or ``pandas.DataFrame``.

    Raises:
        ValueError: If ``backend`` is not ``"polars"`` or ``"pandas"``.
        ImportError: If the chosen backend is not installed.
    """
    if backend == "polars":
        try:
            import polars as pl
        except ImportError as e:
            raise ImportError(
                "polars is not installed. Install it with: pip install 'fungo[polars]'"
            ) from e
        return pl.DataFrame(rows)

    if backend == "pandas":
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "pandas is not installed. Install it with: pip install 'fungo[pandas]'"
            ) from e
        return pd.DataFrame(rows)

    raise ValidationError(backend, "backend", valid_values=["polars", "pandas"])
