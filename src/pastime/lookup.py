"""Lookup player names and IDs.

This module provides functions that allow a user to query the Chadwick Bureau lookup
database with player names and IDs. This is particularly useful when using a function
that requires a player's ID number for a particular service, or when associating an ID
number returned from an API with a particular player.

Attributes:
    LOOKUP_URL (str): URL for the lookup table.
    LOOKUP_COLUMNS (list[str]): Columns to include from the lookup table.
"""

import argparse
import io
from typing import cast

import pkg_resources
import polars as pl

from pastime.download import download_file
from pastime.exceptions import IdNotFoundError, NameNotFoundError


#######################################################################################
# LOOKUP DATA


LOOKUP_URL = (
    "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"
)


LOOKUP_COLUMNS = [
    "name_full",
    "name_first",
    "name_last",
    "key_mlbam",
    "key_retro",
    "key_bbref",
    "key_bbref_minors",
    "key_fangraphs",
    "mlb_played_first",
    "mlb_played_last",
]


#######################################################################################
# LOOKUP METHODS


def get_table(refresh_table: bool = False, mlb_only: bool = False) -> pl.DataFrame:
    """Get Chadwick Bureau lookup table.

    If the table doesn't already exist, it will be downloaded from the original source.
    If the table already exists, it will be retrieved without downloading. If the user
    believes the table is out of date, they can optionally choose to refresh the lookup
    table.

    Args:
        refresh_table (bool, optional): Whether to refresh the lookup table. Defaults
            to False.
        mlb_only (bool, optional): Whether to only include players who have played in
            the majors. Defaults to False.

    Returns:
        pl.DataFrame: Lookup table.
    """
    if (
        pkg_resources.resource_exists(__name__, "data/lookup_table.csv")
        and not refresh_table
    ):
        stream = cast(
            io.BytesIO,
            pkg_resources.resource_stream(__name__, "data/lookup_table.csv"),
        )

        lookup_table = pl.read_csv(stream)

    else:
        # The header is included to fix bug where the content-length for the progress
        # bar is not the same as the actual length of the file received.

        output = download_file(
            url=LOOKUP_URL,
            params={},
            request_name="Lookup Table",
            headers={"Accept-Encoding": "identity"},
        )

        lookup_table = pl.read_csv(output, columns=LOOKUP_COLUMNS[1:])

        # Drops rows only if all values are null

        lookup_table = lookup_table.filter(
            ~pl.fold(
                acc=True,
                f=lambda acc, s: acc & s.is_null(),
                exprs=pl.all(),
            )
        )

        # Needed because when a first (or last) name is null, the full name becomes
        # null even when the last (or first) name is not null.

        first_name = (
            pl.when(pl.col("name_first").is_null())
            .then("")
            .otherwise(pl.col("name_first"))
        )

        last_name = (
            pl.when(pl.col("name_last").is_null())
            .then("")
            .otherwise(pl.col("name_last"))
        )

        lookup_table = lookup_table.with_column(
            (first_name + " " + last_name).alias("name_full")
        )

        lookup_table = lookup_table[LOOKUP_COLUMNS]

    return (
        lookup_table.drop_nulls(["mlb_played_first", "mlb_played_last"])
        if mlb_only
        else lookup_table
    )


def lookup_id(
    player_id: int | str,
    *,
    mlb_only: bool = True,
) -> pl.DataFrame:
    """Get all rows in the lookup table that match the given player ID.

    Args:
        player_id (int | str): The ID to lookup in the database.
        mlb_only (bool, optional): Whether to only include players who have played in
            the majors. Defaults to True.

    Raises:
        IdNotFoundError: If the given ID does not exist in the database for any of the
            included sources.

    Returns:
        pl.DataFrame: Lookup table with all rows that match the given player ID.
    """
    data = get_table(mlb_only=mlb_only).filter(
        (pl.col("key_mlbam").cast(str) == str(player_id))
        | (pl.col("key_fangraphs").cast(str) == str(player_id))
        | (pl.col("key_bbref").cast(str) == str(player_id))
        | (pl.col("key_retro").cast(str) == str(player_id))
    )

    if data.is_empty():
        raise IdNotFoundError(player_id)

    return data


def lookup_name(
    name: str,
    *,
    mlb_only: bool = True,
) -> pl.DataFrame:
    """Get all rows in the lookup table that match the given player name.

    Args:
        name (str): The name to lookup in the database.
        mlb_only (bool, optional): Whether to only include players who have played in
            the majors. Defaults to True.

    Raises:
        NameNotFoundError: If the given name does not exist in the database.

    Returns:
        pl.DataFrame: Lookup table with all rows that match the given player name.
    """
    lookup_table = get_table(mlb_only=mlb_only)

    name = name.strip().lower()

    data = lookup_table.filter(
        (pl.col("name_full").str.to_lowercase() == name)
        | (pl.col("name_first").str.to_lowercase() == name)
        | (pl.col("name_last").str.to_lowercase() == name)
    )

    if data.is_empty():
        raise NameNotFoundError(name)

    return data


def get_name(
    player_id: int | str,
    *,
    mlb_only: bool = True,
) -> str:
    """Get a name associated with a particular player ID.

    Args:
        player_id (int | str): The ID to lookup in the database.
        mlb_only (bool, optional): Whether to only include players who have played in
            the majors. Defaults to True.

    Raises:
        IdNotFoundError: If the given ID does not exist in the database for any of the
            included sources.

    Returns:
        str: Name of the player associated with the given player ID.
    """
    lookup_table = lookup_id(player_id, mlb_only=mlb_only)

    if lookup_table.is_empty():
        raise IdNotFoundError(player_id)

    return lookup_table["name_full"][0]


def get_id(
    name: str,
    source: str = "mlbam",
    start_year: str = None,
    *,
    mlb_only: bool = True,
) -> str:
    """Get an ID associated with a particular player name and source.

    Args:
        name (str): The name to lookup in the database.
        mlb_only (bool, optional): Whether to only include players who have played in
            the majors. Defaults to True.

    Raises:
        ValueError: If the source provided is not 'mlbam', 'fangraphs', 'bbref', or
            'retro'.
        NameNotFoundError: If the given name does not exist in the database.

    Returns:
        str: ID of the player associated with the given player name.
    """
    if source not in ["mlbam", "fangraphs", "bbref", "retro"]:
        raise ValueError(f"Invalid source: '{source}'")

    lookup_table = lookup_name(name, mlb_only=mlb_only)

    if len(lookup_table) > 1 and start_year:
        lookup_table = lookup_table.filter(pl.col("mlb_played_first") == start_year)

    elif len(lookup_table) > 1 and not start_year:
        lookup_table = lookup_table.sort("mlb_played_first")[-1]

    if lookup_table.is_empty():
        raise NameNotFoundError(name)

    return str(lookup_table[f"key_{source}"][0])


def cli():
    """Parse command line arguments and make a request."""
    parser = argparse.ArgumentParser(
        description="Make a Chadwick Bureau database query."
    )

    parser.add_argument("-o", "--output", help="File location to save data")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Whether to refresh the lookup table",
    )
    parser.add_argument(
        "--mlb_only",
        action="store_true",
        help="Whether to get data only for players who have played in the majors",
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument("--id", help="Player ID to lookup")
    group.add_argument("--name", help="Name to lookup")
    group.add_argument("--table", help="Get full lookup table", action="store_true")

    args = vars(parser.parse_args())

    save_location = args.pop("output")

    if player_id := args["id"]:
        data = lookup_id(player_id, mlb_only=args["mlb_only"])

    elif name := args["name"]:
        data = lookup_name(name, mlb_only=args["mlb_only"])

    elif args["table"]:
        data = get_table(refresh_table=args["refresh"], mlb_only=args["mlb_only"])

    if save_location:
        data.write_csv(save_location)

    print(data)


if __name__ == "__main__":
    cli()
