from datetime import date

import polars as pl

from pastime.field import STATCAST_COLLECTIONS, Param
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
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_years=False,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
        year=year,
        start_date=None,
        end_date=None,
        **kwargs,
    )


def dates(
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    *,
    update_years: bool = True,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_years=update_years,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def game(
    game_pk: str | int,
    *,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_years=False,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
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
    update_years: bool = True,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_years=update_years,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
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
    update_years: bool = True,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_years=update_years,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
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
    update_years: bool = True,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_years=update_years,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
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
    update_years: bool = True,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_years=update_years,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
        team=team_name,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def query(
    update_years: bool = True,
    add_spin_columns: bool = True,
    aggregate: bool = False,
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
        collection=STATCAST_COLLECTIONS["search"],
        player_type=player_type,
        min_pitches=min_pitches,
        min_results=min_results,
        group_by=group_by,
        sort_by=sort_by,
        player_event_sort=player_event_sort,
        sort_order="desc",
        min_pa=min_pa,
        data_type="aggregate" if aggregate else "details",
        get_all="true",
        season_type=season_type,
        year=year,
        **kwargs,
    )

    if update_years:
        search_query.update_years()

    data = pl.read_csv(search_query.request(), parse_dates=True, ignore_errors=True)

    if not aggregate:
        data = (
            data.drop_nulls(subset="game_date")
            .drop(DEPRECATED_COLUMNS)
            .sort(["game_date", "game_pk", "at_bat_number", "pitch_number"])
        )

        data = spin_columns(data) if add_spin_columns else data

    return data
