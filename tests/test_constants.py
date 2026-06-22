"""Tests for the constants resolvers (hit + miss)."""

from __future__ import annotations

import pytest

from pastime.constants import (
    TEAM_CODES,
    TEAMS,
    resolve_hand,
    resolve_pitch_type,
    resolve_team,
)
from pastime.exceptions import ValidationError


def test_team_codes_count():
    assert len(TEAM_CODES) == 30
    assert len(TEAMS) == 30


#####################################################################
# resolve_team
#####################################################################


def test_resolve_team_by_code():
    assert resolve_team("LAD") == "LAD"
    assert resolve_team("lad") == "LAD"


def test_resolve_team_by_full_name():
    assert resolve_team("Los Angeles Dodgers") == "LAD"


def test_resolve_team_by_city():
    assert resolve_team("Cincinnati") == "CIN"


def test_resolve_team_by_alias():
    assert resolve_team("Dodgers") == "LAD"
    assert resolve_team("yanks") == "NYY"


def test_resolve_team_miss_raises_with_suggestion():
    with pytest.raises(ValidationError) as exc:
        resolve_team("Dodgrs")
    assert "Did you mean" in str(exc.value)


#####################################################################
# resolve_pitch_type
#####################################################################


def test_resolve_pitch_type_by_code():
    assert resolve_pitch_type("FF") == "FF"
    assert resolve_pitch_type("ff") == "FF"


def test_resolve_pitch_type_by_name():
    assert resolve_pitch_type("4-Seam Fastball") == "FF"


def test_resolve_pitch_type_by_alias():
    assert resolve_pitch_type("4-seam") == "FF"
    assert resolve_pitch_type("sweeper") == "ST"
    assert resolve_pitch_type("splitter") == "FS"


def test_resolve_pitch_type_miss_raises():
    with pytest.raises(ValidationError):
        resolve_pitch_type("banana ball")


#####################################################################
# resolve_hand
#####################################################################


def test_resolve_hand_codes():
    assert resolve_hand("R") == "R"
    assert resolve_hand("L") == "L"
    assert resolve_hand("S") == "S"


def test_resolve_hand_words():
    assert resolve_hand("right") == "R"
    assert resolve_hand("left") == "L"
    assert resolve_hand("switch") == "S"
    assert resolve_hand("both") == "S"


def test_resolve_hand_miss_raises():
    with pytest.raises(ValidationError):
        resolve_hand("sideways")
