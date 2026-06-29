"""MLB Stats API — teams endpoints (directory, history, rosters, stats, leaders)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pastime.exceptions import ValidationError
from pastime.mlb.constants import ROSTER_TYPES, TEAM_IDS, TEAM_IDS_BY_ABBREV
from pastime.mlb.stats_api import _csv, _fmt_date, mlb_api

#####################################################################
# Directory / single team
#####################################################################


def get_teams(
    season: int | str | None = None,
    sport_ids: int | list[int] | None = None,
    active_status: str | None = None,
    all_star_statuses: str | list[str] | None = None,
    league_ids: int | list[int] | None = None,
    game_type: str | None = None,
    hydrate: str | None = None,
    fields: str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams`` — directory of teams.

    Args:
        season: Season year.
        sport_ids: Sport/level id(s) to filter by.
        active_status: Active-status filter (e.g. ``Y``, ``N``, ``B``).
        all_star_statuses: All-star status filter(s).
        league_ids: League id(s) to filter by.
        game_type: Game-type code.
        hydrate: Optional hydration string.
        fields: Optional sparse-field selection string.

    Returns:
        The raw JSON ``teams`` payload.
    """
    params = {
        "season": season,
        "sportIds": _csv(sport_ids),
        "activeStatus": active_status,
        "allStarStatuses": _csv(all_star_statuses),
        "leagueIds": _csv(league_ids),
        "gameType": game_type,
        "hydrate": hydrate,
        "fields": fields,
    }
    return mlb_api("/api/v1/teams", params)


def get_team(
    team_id: int | str,
    season: int | str | None = None,
    sport_id: int | str | None = None,
    hydrate: str | None = None,
    fields: str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/{teamId}`` — a single team.

    Args:
        team_id: MLB team id.
        season: Season year.
        sport_id: Sport/level id.
        hydrate: Optional hydration string.
        fields: Optional sparse-field selection string.

    Returns:
        The raw JSON payload for the team.
    """
    params = {
        "season": season,
        "sportId": sport_id,
        "hydrate": hydrate,
        "fields": fields,
    }
    return mlb_api(f"/api/v1/teams/{team_id}", params)


def get_team_history(
    team_ids: int | str | list[int | str],
    start_season: int | str | None = None,
    end_season: int | str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/history`` — historical records for teams.

    Args:
        team_ids: One or more MLB team ids.
        start_season: First season of the range.
        end_season: Last season of the range.

    Returns:
        The raw JSON history payload.
    """
    params = {
        "teamIds": _csv(team_ids),
        "startSeason": start_season,
        "endSeason": end_season,
    }
    return mlb_api("/api/v1/teams/history", params)


#####################################################################
# Stats / leaders
#####################################################################


def get_team_stats(
    season: int | str,
    stat_group: str,
    stats: str,
    sport_ids: int | list[int] | None = None,
    game_type: str | None = None,
    order: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
    sort_stat: str | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    sit_codes: str | list[str] | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/stats`` — team statistics (league-wide or filtered).

    Args:
        season: Season year.
        stat_group: Stat group (e.g. ``hitting``, ``pitching``).
        stats: Stat type (e.g. ``season``, ``career``).
        sport_ids: Sport/level id(s) to filter by.
        game_type: Game-type code.
        order: Sort direction.
        limit: Page size.
        offset: Page offset.
        sort_stat: Stat field to sort by.
        start_date: Range start (formatted to ``MM/DD/YYYY``).
        end_date: Range end (formatted to ``MM/DD/YYYY``).
        sit_codes: Situation split code(s).

    Returns:
        The raw JSON team-stats payload.
    """
    params = {
        "season": season,
        "group": stat_group,
        "stats": stats,
        "sportIds": _csv(sport_ids),
        "gameType": game_type,
        "order": order,
        "limit": limit,
        "offset": offset,
        "sortStat": sort_stat,
        "startDate": _fmt_date(start_date),
        "endDate": _fmt_date(end_date),
        "sitCodes": _csv(sit_codes),
    }
    return mlb_api("/api/v1/teams/stats", params)


def get_team_leaders(
    leader_categories: str | list[str],
    season: int | str | None = None,
    sport_id: int | str | None = None,
    game_types: str | list[str] | None = None,
    stat_group: str | None = None,
    league_ids: int | list[int] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    stat_type: str | None = None,
    sit_codes: str | list[str] | None = None,
    hydrate: str | None = None,
    limit: int | None = None,
    fields: str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/stats/leaders`` — league-wide team-stat leaders.

    Args:
        leader_categories: Leader category name(s).
        season: Season year.
        sport_id: Sport/level id.
        game_types: Game-type code(s).
        stat_group: Stat group.
        league_ids: League id(s).
        start_date: Range start (formatted to ``MM/DD/YYYY``).
        end_date: Range end (formatted to ``MM/DD/YYYY``).
        stat_type: Stat type.
        sit_codes: Situation split code(s).
        hydrate: Optional hydration string.
        limit: Page size.
        fields: Optional sparse-field selection string.

    Returns:
        The raw JSON team-leaders payload.
    """
    params = {
        "leaderCategories": _csv(leader_categories),
        "season": season,
        "sportId": sport_id,
        "gameTypes": _csv(game_types),
        "statGroup": stat_group,
        "leagueIds": _csv(league_ids),
        "startDate": _fmt_date(start_date),
        "endDate": _fmt_date(end_date),
        "statType": stat_type,
        "sitCodes": _csv(sit_codes),
        "hydrate": hydrate,
        "limit": limit,
        "fields": fields,
    }
    return mlb_api("/api/v1/teams/stats/leaders", params)


def get_team_affiliates(
    team_ids: int | str | list[int | str],
    sport_id: int | str | None = None,
    season: int | str | None = None,
    hydrate: str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/affiliates`` — affiliate (farm system) teams.

    Args:
        team_ids: One or more parent-club team ids.
        sport_id: Sport/level id.
        season: Season year.
        hydrate: Optional hydration string.

    Returns:
        The raw JSON affiliates payload.
    """
    params = {
        "teamIds": _csv(team_ids),
        "sportId": sport_id,
        "season": season,
        "hydrate": hydrate,
    }
    return mlb_api("/api/v1/teams/affiliates", params)


#####################################################################
# Roster
#####################################################################


def get_roster(
    team_id: int | str,
    roster_type: str | None = None,
    season: int | str | None = None,
    date: str | date | datetime | None = None,
    game_type: str | None = None,
    hydrate: str | None = None,
    fields: str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/{teamId}/roster``.

    Gotcha: when hydrating ``rosterEntries`` elsewhere, its ``sportId`` argument
    is silently ignored — every variant returns the same entries. Do not try to
    scope ``rosterEntries`` by sport; hydrate it bare and inspect the result.

    Args:
        team_id: MLB team id.
        roster_type: One of :data:`pastime.mlb.constants.ROSTER_TYPES`.
        season: Season year.
        date: As-of date (formatted to ``MM/DD/YYYY``).
        game_type: Game-type code.
        hydrate: Optional hydration string.
        fields: Optional sparse-field selection string.

    Returns:
        The raw JSON roster payload.

    Raises:
        ValidationError: If ``roster_type`` is not a known roster type.
    """
    if roster_type is not None and roster_type not in ROSTER_TYPES:
        raise ValidationError(roster_type, "roster_type", list(ROSTER_TYPES))
    params = {
        "rosterType": roster_type,
        "season": season,
        "date": _fmt_date(date),
        "gameType": game_type,
        "hydrate": hydrate,
        "fields": fields,
    }
    return mlb_api(f"/api/v1/teams/{team_id}/roster", params)


#####################################################################
# Team-specific endpoints
#####################################################################


def get_team_specific_leaders(
    team_id: int | str,
    leader_categories: str | list[str],
    season: int | str | None = None,
    leader_game_types: str | list[str] | None = None,
    hydrate: str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/{teamId}/leaders`` — team-specific leaders.

    Args:
        team_id: MLB team id.
        leader_categories: Leader category name(s).
        season: Season year.
        leader_game_types: Game-type code(s).
        hydrate: Optional hydration string.

    Returns:
        The raw JSON team-leaders payload.
    """
    params = {
        "leaderCategories": _csv(leader_categories),
        "season": season,
        "leaderGameTypes": _csv(leader_game_types),
        "hydrate": hydrate,
    }
    return mlb_api(f"/api/v1/teams/{team_id}/leaders", params)


def get_team_specific_stats(
    team_id: int | str,
    season: int | str | None = None,
    group: str | None = None,
    stats: str | None = None,
    game_type: str | None = None,
    sport_ids: int | list[int] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    sit_codes: str | list[str] | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/{teamId}/stats`` — stats for a specific team.

    Args:
        team_id: MLB team id.
        season: Season year.
        group: Stat group.
        stats: Stat type.
        game_type: Game-type code.
        sport_ids: Sport/level id(s).
        start_date: Range start (formatted to ``MM/DD/YYYY``).
        end_date: Range end (formatted to ``MM/DD/YYYY``).
        sit_codes: Situation split code(s).

    Returns:
        The raw JSON team-stats payload.
    """
    params = {
        "season": season,
        "group": group,
        "stats": stats,
        "gameType": game_type,
        "sportIds": _csv(sport_ids),
        "startDate": _fmt_date(start_date),
        "endDate": _fmt_date(end_date),
        "sitCodes": _csv(sit_codes),
    }
    return mlb_api(f"/api/v1/teams/{team_id}/stats", params)


def get_team_alumni(
    team_id: int | str,
    season: int | str,
    group: str | None = None,
    hydrate: str | None = None,
) -> dict[str, Any]:
    """Fetch ``/api/v1/teams/{teamId}/alumni`` — alumni for a team.

    Args:
        team_id: MLB team id.
        season: Season year.
        group: Stat group.
        hydrate: Optional hydration string.

    Returns:
        The raw JSON alumni payload.
    """
    params = {"season": season, "group": group, "hydrate": hydrate}
    return mlb_api(f"/api/v1/teams/{team_id}/alumni", params)


#####################################################################
# Resolver
#####################################################################


def resolve_team_id(name_or_abbrev: str) -> int | None:
    """Resolve a team name or abbreviation to an MLB team id, or ``None``.

    Case-insensitive. Checks the full-name table first, then the abbreviation
    table, then a case-insensitive full-name sweep.

    Args:
        name_or_abbrev: A team nickname, full market name, or abbreviation.

    Returns:
        The MLB team id, or ``None`` if no match.
    """
    if not name_or_abbrev:
        return None
    s = str(name_or_abbrev).strip()

    # Exact full name
    if s in TEAM_IDS:
        return TEAM_IDS[s]
    # Exact abbreviation
    up = s.upper()
    if up in TEAM_IDS_BY_ABBREV:
        return TEAM_IDS_BY_ABBREV[up]
    # Case-insensitive full-name
    low = s.lower()
    for name, tid in TEAM_IDS.items():
        if name.lower() == low:
            return tid
    return None
