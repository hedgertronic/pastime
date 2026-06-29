"""Tests for the optional DataFrame conversion.

polars and pandas are both installed in the dev env, so the happy paths run
offline. An unknown backend must raise ValidationError before any import.
"""

from __future__ import annotations

import builtins

import pytest

from fungo.exceptions import ValidationError
from fungo.frame import to_frame

_ROWS = [
    {"key_mlbam": "545361", "name_last": "Trout"},
    {"key_mlbam": "660271", "name_last": "Ohtani"},
]


def test_to_frame_polars():
    import polars as pl

    frame = to_frame(_ROWS, backend="polars")
    assert isinstance(frame, pl.DataFrame)
    assert frame.shape == (2, 2)
    assert frame.columns == ["key_mlbam", "name_last"]
    assert frame["key_mlbam"].to_list() == ["545361", "660271"]


def test_to_frame_pandas():
    import pandas as pd

    frame = to_frame(_ROWS, backend="pandas")
    assert isinstance(frame, pd.DataFrame)
    assert frame.shape == (2, 2)
    assert list(frame.columns) == ["key_mlbam", "name_last"]
    assert frame["key_mlbam"].tolist() == ["545361", "660271"]


def test_to_frame_default_backend_is_polars():
    import polars as pl

    assert isinstance(to_frame(_ROWS), pl.DataFrame)


def test_to_frame_unknown_backend_raises():
    with pytest.raises(ValidationError) as exc_info:
        to_frame(_ROWS, backend="numpy")
    # ValidationError subclasses ValueError.
    assert isinstance(exc_info.value, ValueError)
    assert exc_info.value.field_name == "backend"
    assert exc_info.value.valid_values == ["polars", "pandas"]
    assert "numpy" in str(exc_info.value)


def test_to_frame_empty_polars():
    import polars as pl

    frame = to_frame([], backend="polars")
    assert isinstance(frame, pl.DataFrame)
    assert frame.shape == (0, 0)


def test_to_frame_empty_pandas():
    import pandas as pd

    frame = to_frame([], backend="pandas")
    assert isinstance(frame, pd.DataFrame)
    assert frame.empty


def test_to_frame_missing_polars_raises_actionable_import_error(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "polars":
            raise ImportError("no polars")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match=r"fungo\[polars\]"):
        to_frame(_ROWS, backend="polars")


def test_to_frame_missing_pandas_raises_actionable_import_error(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "pandas":
            raise ImportError("no pandas")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match=r"fungo\[pandas\]"):
        to_frame(_ROWS, backend="pandas")
