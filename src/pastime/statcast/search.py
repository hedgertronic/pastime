"""
Make requests to the Statcast database.

This module provides a set of functions that allow for querying and downloading data
from the Baseball Savant Statcast Search database.

The web version of the database can be found at
https://baseballsavant.mlb.com/statcast_search.

An explanation of the all the fields and values returned from the databse can be found
at https://baseballsavant.mlb.com/csv-docs.
"""

import argparse
from datetime import date

import polars as pl

from pastime.field import Param
from pastime.statcast.analysis import spin_columns
from pastime.statcast.base import STATCAST_COLLECTIONS
from pastime.statcast.query import SearchQuery


#######################################################################################
# USEFUL CONSTANTS


# The base URL for Baseball Savant
URL = "https://baseballsavant.mlb.com"


# Columns that are deprecated and should not be included in requests
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


#######################################################################################
# STATCAST SEARCH FUNCTIONS


def season(
    year: Param,
    *,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    """Get Statcast data for a full season.

    This will require many separate requests and will take a while.

    Args:
        year (Param): The year to get data for.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
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
    """Get Statcast for a certain range of dates.

    Args:
        start_date (str | date | None, optional): The earliest date to get in the
            request. Defaults to today's date or end date if provided.
        end_date (str | date | None, optional): The latest date to get in the request.
            Defaults to today's date or start date if provided.
        update_years (bool, optional): Whether to update the years included in the
            request to reflect the dates provided in the request. Defaults to True.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
    if not start_date and end_date:
        end_date = date.today()
        start_date = date.today()

    return query(
        update_years=update_years,
        add_spin_columns=add_spin_columns,
        aggregate=aggregate,
        start_date=start_date or end_date,
        end_date=end_date or start_date,
        **kwargs,
    )


def game(
    game_pk: str | int,
    *,
    add_spin_columns: bool = True,
    aggregate: bool = False,
    **kwargs: Param,
) -> pl.DataFrame:
    """Get Statcast data for an individual game.

    Args:
        game_pk (str | int): The primary key of the game to retrieve.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
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
    """Get Statcast data for a pitcher or list of pitchers.

    Args:
        pitchers (Param): The pitcher ID or IDs to retrieve data for.
        start_date (str | date | None, optional): The earliest date to get in the
            request. Defaults to the start date of the earliest season in the request.
        end_date (str | date | None, optional): The latest date to get in the request.
            Defaults to the end date of the latest season in the request.
        update_years (bool, optional): Whether to update the years included in the
            request to reflect the dates provided in the request. Defaults to True.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
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
    """Get Statcast data for a batter or list of batters.

    Args:
        batter (Param): The batter ID or IDs to retrieve data for.
        start_date (str | date | None, optional): The earliest date to get in the
            request. Defaults to the start date of the earliest season in the request.
        end_date (str | date | None, optional): The latest date to get in the request.
            Defaults to the end date of the latest season in the request.
        update_years (bool, optional): Whether to update the years included in the
            request to reflect the dates provided in the request. Defaults to True.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
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
    """Get Statcast data for batter/pitcher matchups.

    Args:
        pitchers (Param): The pitcher ID or IDs to retrieve data for.
        batters (Param): The batter ID or IDs to retrieve data for.
        start_date (str | date | None, optional): The earliest date to get in the
            request. Defaults to the start date of the earliest season in the request.
        end_date (str | date | None, optional): The latest date to get in the request.
            Defaults to the end date of the latest season in the request.
        update_years (bool, optional): Whether to update the years included in the
            request to reflect the dates provided in the request. Defaults to True.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
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
    """Get Statcast data for a team.

    Args:
        team (str): The team to get data for.
        start_date (str | date | None, optional): The earliest date to get in the
            request. Defaults to the start date of the earliest season in the request.
        end_date (str | date | None, optional): The latest date to get in the request.
            Defaults to the end date of the latest season in the request.
        update_years (bool, optional): Whether to update the years included in the
            request to reflect the dates provided in the request. Defaults to True.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
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
    """Make a Statcast search query.

    Args:
        update_years (bool, optional): Whether to update the years included in the
            request to reflect the dates provided in the request. Defaults to True.
        add_spin_columns (bool, optional): Whether to add spin columns to the data.
            Defaults to True.
        aggregate (bool, optional): Whether to aggregate the data or keep invididual
            pitch-by-pitch. Defaults to False.
        player_type (Param, optional): The player type to set the `player_type` field
            in the returned data. Defaults to "pitcher".
        min_pitches (Param, optional): The minimum number of pitches. Defaults to "0".
        min_results (Param, optional): The minimum number of results. Defaults to "0".
        group_by (Param, optional): Field to group by. Defaults to "player name".
        sort_by (Param, optional): Field to sort by. Defaults to "pitches".
        player_event_sort (Param, optional): Field to sort individual player events.
            Defaults to "exit velocity".
        min_pa (Param, optional): The minimum number of PAs. Defaults to "0".
        season_type (Param, optional): The type of season. Defaults to "regular season".
        year (Param, optional): The year. Defaults to "2022".
        kwargs (Param, optional): Additional params to include in the request.

    Returns:
        pl.DataFrame: The returned, cleaned, and sorted data.
    """
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


def cli():
    """Parse command line arguments and make a request."""
    parser = argparse.ArgumentParser(description="Make a Statcast search query.")

    parser.add_argument(
        "-o", "--output", required=True, help="File location to save data"
    )
    parser.add_argument("--year")
    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--team")
    parser.add_argument("--game_pk")
    parser.add_argument("--batters")
    parser.add_argument("--pitchers")

    _, unknown = parser.parse_known_args()

    for arg in unknown:
        if arg.startswith(("--")):
            parser.add_argument(arg.split("=", maxsplit=1)[0])

    args = vars(parser.parse_args())

    save_location = args.pop("output")

    valid_args = {
        arg_key: str(arg_value).split(",")
        for arg_key, arg_value in args.items()
        if arg_value
    }

    data = query(**valid_args)
    data.write_csv(save_location)

    print(data)


if __name__ == "__main__":
    cli()
