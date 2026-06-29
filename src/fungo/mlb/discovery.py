"""MLB Stats API — discovery (meta) endpoints.

These meta-endpoints return the valid values for parameters used elsewhere in
the API — stat types, game types, positions, situation codes, etc. They are
useful for introspecting what the live API currently accepts (the local tables
in :mod:`fungo.mlb.constants` are a frozen mirror).
"""

from __future__ import annotations

from typing import Any

from fungo.mlb.stats_api import mlb_api

#####################################################################
# Discovery endpoints
#####################################################################


def get_stat_types() -> dict[str, Any]:
    """Fetch ``/api/v1/statTypes`` — all stat type names.

    Returns:
        The raw JSON stat-types payload.
    """
    return mlb_api("/api/v1/statTypes")


def get_stat_groups() -> dict[str, Any]:
    """Fetch ``/api/v1/statGroups`` — all stat group names.

    Returns:
        The raw JSON stat-groups payload.
    """
    return mlb_api("/api/v1/statGroups")


def get_game_types() -> dict[str, Any]:
    """Fetch ``/api/v1/gameTypes`` — all game type codes.

    Returns:
        The raw JSON game-types payload.
    """
    return mlb_api("/api/v1/gameTypes")


def get_positions() -> dict[str, Any]:
    """Fetch ``/api/v1/positions`` — all positions.

    Returns:
        The raw JSON positions payload.
    """
    return mlb_api("/api/v1/positions")


def get_situation_codes() -> dict[str, Any]:
    """Fetch ``/api/v1/situationCodes`` — all situation split codes.

    Returns:
        The raw JSON situation-codes payload.
    """
    return mlb_api("/api/v1/situationCodes")


def get_metrics() -> dict[str, Any]:
    """Fetch ``/api/v1/metrics`` — all Statcast metrics.

    Returns:
        The raw JSON metrics payload.
    """
    return mlb_api("/api/v1/metrics")


def get_league_leader_types() -> dict[str, Any]:
    """Fetch ``/api/v1/leagueLeaderTypes`` — all leader categories.

    Returns:
        The raw JSON leader-types payload.
    """
    return mlb_api("/api/v1/leagueLeaderTypes")


def get_sports_discovery() -> dict[str, Any]:
    """Fetch ``/api/v1/sports`` — all sport IDs and levels.

    Returns:
        The raw JSON sports payload.
    """
    return mlb_api("/api/v1/sports")


def get_baseball_stats() -> dict[str, Any]:
    """Fetch ``/api/v1/baseballStats`` — all stat field names.

    Returns:
        The raw JSON baseball-stats payload.
    """
    return mlb_api("/api/v1/baseballStats")


def get_roster_types() -> dict[str, Any]:
    """Fetch ``/api/v1/rosterTypes`` — all roster types.

    Returns:
        The raw JSON roster-types payload.
    """
    return mlb_api("/api/v1/rosterTypes")


def get_standings_types() -> dict[str, Any]:
    """Fetch ``/api/v1/standingsTypes`` — all standings types.

    Returns:
        The raw JSON standings-types payload.
    """
    return mlb_api("/api/v1/standingsTypes")


def get_game_status() -> dict[str, Any]:
    """Fetch ``/api/v1/gameStatus`` — all game status codes.

    Returns:
        The raw JSON game-status payload.
    """
    return mlb_api("/api/v1/gameStatus")


def get_pitch_types() -> dict[str, Any]:
    """Fetch ``/api/v1/pitchTypes`` — all pitch type codes.

    Returns:
        The raw JSON pitch-types payload.
    """
    return mlb_api("/api/v1/pitchTypes")


def get_hit_trajectories() -> dict[str, Any]:
    """Fetch ``/api/v1/hitTrajectories`` — all batted-ball trajectories.

    Returns:
        The raw JSON hit-trajectories payload.
    """
    return mlb_api("/api/v1/hitTrajectories")


def get_event_types() -> dict[str, Any]:
    """Fetch ``/api/v1/eventTypes`` — all play event types.

    Returns:
        The raw JSON event-types payload.
    """
    return mlb_api("/api/v1/eventTypes")


def get_schedule_event_types() -> dict[str, Any]:
    """Fetch ``/api/v1/scheduleEventTypes`` — all schedule event types.

    Returns:
        The raw JSON schedule-event-types payload.
    """
    return mlb_api("/api/v1/scheduleEventTypes")


def get_wind_direction() -> dict[str, Any]:
    """Fetch ``/api/v1/windDirection`` — wind direction codes.

    Returns:
        The raw JSON wind-direction payload.
    """
    return mlb_api("/api/v1/windDirection")


def get_sky() -> dict[str, Any]:
    """Fetch ``/api/v1/sky`` — sky condition codes.

    Returns:
        The raw JSON sky-conditions payload.
    """
    return mlb_api("/api/v1/sky")


def get_job_types() -> dict[str, Any]:
    """Fetch ``/api/v1/jobTypes`` — all job/coach types.

    Returns:
        The raw JSON job-types payload.
    """
    return mlb_api("/api/v1/jobTypes")


def get_languages() -> dict[str, Any]:
    """Fetch ``/api/v1/languages`` — supported languages.

    Returns:
        The raw JSON languages payload.
    """
    return mlb_api("/api/v1/languages")


def get_platforms() -> dict[str, Any]:
    """Fetch ``/api/v1/platforms`` — platform identifiers.

    Returns:
        The raw JSON platforms payload.
    """
    return mlb_api("/api/v1/platforms")
