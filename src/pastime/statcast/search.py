from datetime import date

import polars as pl

from pastime.statcast.field import Param
from pastime.statcast.query import SearchQuery


def season(year: Param, **kwargs: Param) -> pl.DataFrame:
    return query(
        update_seasons=False, season=year, start_date=None, end_date=None, **kwargs
    )


def dates(
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    *,
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def game(game_pk: str | int, **kwargs: Param) -> pl.DataFrame:
    return query(
        update_seasons=False,
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
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
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
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
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
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
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
    update_seasons: bool = True,
    **kwargs: Param,
) -> pl.DataFrame:
    return query(
        update_seasons=update_seasons,
        team=team_name,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )


def query(
    update_seasons: bool = True,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    **kwargs,
) -> pl.DataFrame:
    if (start_date or end_date) and kwargs.get("date_range"):
        raise ValueError("Cannot specify both start/end date and date range.")

    search_query = SearchQuery(date_range=[start_date, end_date], **kwargs)

    if update_seasons:
        search_query.update_seasons()

    return search_query.request()
