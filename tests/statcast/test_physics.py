"""Offline tests for fungo.statcast.physics (pure math, no network)."""

from __future__ import annotations

from fungo.statcast import physics
from fungo.statcast.physics import DERIVED_COLUMNS, add_spin_columns, axis_to_clock

# Realistic 4-seam fastball release vectors (Statcast-scale ft/s & ft/s^2).
# Verified to yield a valid physical solution (compute_row returns non-None).
PITCH_ROW = {
    "release_extension": "6.5",
    "vx0": "5.0",
    "vy0": "-135.0",
    "vz0": "-5.0",
    "ax": "-10.0",
    "ay": "30.0",
    "az": "-15.0",
    "spinx": "50.0",
    "spiny": "-2200.0",
    "spinz": "600.0",
}


#####################################################################
# add_spin_columns
#####################################################################


def test_add_spin_columns_on_known_row():
    rows = add_spin_columns([dict(PITCH_ROW)])
    assert len(rows) == 1
    out = rows[0]
    # all derived keys present
    for col in DERIVED_COLUMNS:
        assert col in out
    # known computed values (from compute_row on this exact vector)
    assert round(out["spin_rate"], 2) == 2280.90
    assert round(out["vbar"], 2) == 130.07
    assert round(out["tF"], 4) == 0.4051
    assert 0.0 <= out["spin_eff"] <= 1.0
    assert round(out["spin_eff"], 4) == 0.3189


def test_add_spin_columns_leaves_incomplete_row_untouched():
    incomplete = {"release_extension": "6.5", "vx0": "5.0"}  # missing inputs
    rows = add_spin_columns([incomplete])
    assert rows[0] == incomplete
    assert "spin_rate" not in rows[0]


def test_add_spin_columns_not_inplace_by_default():
    src = dict(PITCH_ROW)
    add_spin_columns([src])
    assert "spin_rate" not in src  # original untouched


def test_add_spin_columns_inplace_mutates():
    src = dict(PITCH_ROW)
    add_spin_columns([src], inplace=True)
    assert "spin_rate" in src


#####################################################################
# Defensive branches
#####################################################################


def test_to_float_returns_none_for_non_numeric():
    assert physics._to_float("not-a-number") is None


def test_time_in_air_returns_none_for_negative_discriminant():
    assert (
        physics._time_in_air(
            velocity=0.0,
            acceleration=1.0,
            position=10.0,
            adjustment=0.0,
            positive=True,
        )
        is None
    )


def test_compute_row_returns_none_for_missing_required_input():
    row = dict(PITCH_ROW)
    del row["spinx"]

    assert physics.compute_row(row) is None


def test_compute_row_returns_none_for_zero_spin_vector():
    row = dict(PITCH_ROW, spinx="0", spiny="0", spinz="0")

    assert physics.compute_row(row) is None


def test_compute_row_returns_none_when_release_time_has_no_solution():
    row = dict(PITCH_ROW, vy0="0", ay="-100")

    assert physics.compute_row(row) is None


def test_compute_row_returns_none_when_flight_time_has_no_solution():
    row = dict(PITCH_ROW, vy0="-10", ay="2")

    assert physics.compute_row(row) is None


def test_compute_row_returns_none_for_zero_average_velocity(monkeypatch):
    times = iter([0.0, 0.0])
    monkeypatch.setattr(physics, "_time_in_air", lambda *args, **kwargs: next(times))
    row = dict(PITCH_ROW, vx0="0", vy0="0", vz0="0")

    assert physics.compute_row(row) is None


def test_compute_row_returns_none_for_zero_transverse_acceleration(monkeypatch):
    times = iter([0.0, 0.0])
    monkeypatch.setattr(physics, "_time_in_air", lambda *args, **kwargs: next(times))
    row = dict(PITCH_ROW, vx0="10", vy0="0", vz0="0", ax="1", ay="0", az="-32.174")

    assert physics.compute_row(row) is None


def test_compute_row_returns_none_for_zero_spin_efficiency(monkeypatch):
    times = iter([0.0, 0.0])
    monkeypatch.setattr(physics, "_time_in_air", lambda *args, **kwargs: next(times))
    row = dict(
        PITCH_ROW,
        vx0="10",
        vy0="0",
        vz0="0",
        ax="0",
        ay="1",
        az="0",
        spinx="100",
        spiny="0",
        spinz="0",
    )

    assert physics.compute_row(row) is None


#####################################################################
# axis_to_clock
#####################################################################


def test_axis_to_clock_cardinals():
    assert axis_to_clock(0) == "12:00"
    assert axis_to_clock(90) == "3:00"
    assert axis_to_clock(180) == "6:00"
    assert axis_to_clock(270) == "9:00"
    assert axis_to_clock(45) == "1:30"


def test_axis_to_clock_none():
    assert axis_to_clock(None) is None


def test_axis_to_clock_accepts_string():
    assert axis_to_clock("90") == "3:00"


def test_axis_to_clock_rolls_rounded_sixty_minutes_to_next_hour():
    assert axis_to_clock(29.8) == "1:00"
