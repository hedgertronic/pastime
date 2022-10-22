from datetime import date

import polars as pl

from pastime.field import Param
from pastime.statcast.analysis import compute_spin_columns
from pastime.statcast.query import SearchQuery


URL = "https://baseballsavant.mlb.com/statcast_search/csv?"


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
    year: Param, *, add_spin_columns: bool = True, **kwargs: Param
) -> pl.DataFrame:
    return query(
        update_seasons=False,
        add_spin_columns=add_spin_columns,
        season=year,
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
    game_pk: str | int, *, add_spin_columns: bool = True, **kwargs: Param
) -> pl.DataFrame:
    return query(
        update_seasons=False,
        add_spin_columns=add_spin_columns,
        game_pk=game_pk,
        season="all years",
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
    **kwargs,
) -> pl.DataFrame:
    search_query = SearchQuery(url=URL, **kwargs)

    if update_seasons:
        search_query.update_seasons()

    data = (
        pl.read_csv(search_query.request(), parse_dates=True, ignore_errors=True)
        .drop_nulls(subset="game_date")
        .drop(DEPRECATED_COLUMNS)
        .sort(["game_date", "game_pk", "at_bat_number", "pitch_number"])
    )

    return compute_spin_columns(data) if add_spin_columns else data
