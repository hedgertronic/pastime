from datetime import date

import polars as pl

from pastime.field import STATCAST_FIELDS, Param
from pastime.statcast.analysis import spin_columns
from pastime.statcast.query import SearchQuery


URL = "https://baseballsavant.mlb.com"


DEPRECATED_COLUMNS = [
    "spin_dir",
    "spin_rate_deprecated",
    "break_angle_deprecated",
    "break_length_deprecated",
    "tfs_deprecated",
    "tfs_zulu_deprecated",
    "umpire",
    "pitcher_duplicated_0",
    "fielder_2_duplicated_0",
]


def season(
    year: Param,
    *,
    add_spin_columns: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=False,
        add_spin_columns=add_spin_columns,
        year=year,
        start_date=None,
        end_date=None,
        **kwargs,
    )


def dates(
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    *,
    add_spin_columns: bool = True,
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
        add_spin_columns=add_spin_columns,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def game(
    game_pk: str | int,
    *,
    add_spin_columns: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=False,
        add_spin_columns=add_spin_columns,
        game_pk=game_pk,
        year="all years",
        start_date=None,
        end_date=None,
        **kwargs,
    )


def pitcher(
    pitchers: Param,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    *,
    add_spin_columns: bool = True,
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
        add_spin_columns=add_spin_columns,
        pitchers=pitchers,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def batter(
    batters: Param,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    *,
    add_spin_columns: bool = True,
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
        add_spin_columns=add_spin_columns,
        batters=batters,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def matchup(
    pitchers: Param,
    batters: Param,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    *,
    add_spin_columns: bool = True,
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
        add_spin_columns=add_spin_columns,
        pitchers=pitchers,
        batters=batters,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def team(
    team_name: str,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    *,
    add_spin_columns: bool = True,
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
        add_spin_columns=add_spin_columns,
        team=team_name,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def query(
    update_seasons: bool = True,
    add_spin_columns: bool = True,
    *,
    player_type: Param = "pitcher",
    min_pitches: Param = "0",
    min_results: Param = "0",
    group_by: Param = "player name",
    sort_by: Param = "pitches",
    player_event_sort: Param = "exit velocity",
    min_pa: Param = "0",
    season_type: Param = "regular season",
    year: Param = "2022",
    **kwargs: Param,
) -> pl.DataFrame:
    search_query = SearchQuery(
        url=URL,
        collection_name="search",
        fields=STATCAST_FIELDS,
        player_type=player_type,
        min_pitches=min_pitches,
        min_results=min_results,
        group_by=group_by,
        sort_by=sort_by,
        player_event_sort=player_event_sort,
        sort_order="desc",
        min_pa=min_pa,
        data_type="details",
        get_all="true",
        season_type=season_type,
        year=year,
        **kwargs,
    )

    if update_seasons:
        search_query.update_seasons()

    data = (
        pl.read_csv(search_query.request(), parse_dates=True, ignore_errors=True)
        .drop_nulls(subset="game_date")
        .drop(DEPRECATED_COLUMNS)
        .sort(["game_date", "game_pk", "at_bat_number", "pitch_number"])
    )

    return spin_columns(data) if add_spin_columns else data
