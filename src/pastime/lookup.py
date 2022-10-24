"""Lookup player names and IDs.

This module provides functions that allow a user to query the Chadwick Bureau lookup
database with player names and IDs. This is particularly useful when using a function
that requires a player's ID number for a particular service, or when associating an ID
number returned from an API with a particular player.

Attributes:
    LOOKUP_URL (str): URL for the lookup table.
    LOOKUP_COLUMNS (list[str]): Columns to include from the lookup table.
"""

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


def get_lookup_table(refresh_table: bool = False) -> pl.DataFrame:
    """Get Chadwick Bureau lookup table.

    If the table doesn't already exist, it will be downloaded and saved to a CSV. If
    the table already exists, it will be retrieved without downloading. If the user
    believes the table is out of date, they can optionally choose to refresh the lookup
    table and replace the existing file.

    Args:
        refresh_table (bool, optional): Whether to refresh the lookup table. Defaults
            to False.

    Returns:
        pl.DataFrame: Lookup table.
    """
    if (
        not pkg_resources.resource_exists(__name__, "data/lookup_table.csv")
        or refresh_table
    ):
        output = io.StringIO()

        # The header is included to fix bug where the content-length for the progress
        # bar is not the same as the actual length of the file received.

        output = download_file(
            url=LOOKUP_URL,
            output=output,
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
        lookup_table.write_csv("data/lookup_table.csv")
    else:
        stream = cast(
            io.BytesIO,
            pkg_resources.resource_stream(__name__, "data/lookup_table.csv"),
        )

        lookup_table = pl.read_csv(stream)

    return lookup_table


def lookup_id(player_id: int | str, *, refresh_table: bool = False) -> pl.DataFrame:
    """Get all rows in the lookup table that match the given player ID.

    Args:
        player_id (int | str): The ID to lookup in the database.
        refresh_table (bool, optional): Whether to refresh the lookup table. Defaults
            to False.

    Raises:
        IdNotFoundError: If the given ID does not exist in the database for any of the
            included sources.

    Returns:
        pl.DataFrame: Lookup table with all rows that match the given player ID.
    """
    data = get_lookup_table(refresh_table=refresh_table).filter(
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
    refresh_table: bool = False,
) -> pl.DataFrame:
    """Get all rows in the lookup table that match the given player name.

    Args:
        name (str): The name to lookup in the database.
        refresh_table (bool, optional): Whether to refresh the lookup table. Defaults
            to False.

    Raises:
        NameNotFoundError: If the given name does not exist in the database.

    Returns:
        pl.DataFrame: Lookup table with all rows that match the given player name.
    """
    lookup_table = get_lookup_table(refresh_table=refresh_table)

    name = name.strip().lower()

    data = lookup_table.filter(
        (pl.col("name_full").str.to_lowercase() == name)
        | (pl.col("name_first").str.to_lowercase() == name)
        | (pl.col("name_last").str.to_lowercase() == name)
    )

    if data.is_empty():
        raise NameNotFoundError(name)

    return data


def get_name(player_id: int | str, *, refresh_table: bool = False) -> str:
    """Get a name associated with a particular player ID.

    Args:
        player_id (int | str): The ID to lookup in the database.
        refresh_table (bool, optional): Whether to refresh the lookup table. Defaults
            to False.

    Raises:
        IdNotFoundError: If the given ID does not exist in the database for any of the
            included sources.

    Returns:
        str: Name of the player associated with the given player ID.
    """
    table = lookup_id(player_id, refresh_table=refresh_table)

    if table.is_empty():
        raise IdNotFoundError(player_id)

    return table["name_full"][0]


def get_id(
    name: str,
    source: str = "mlbam",
    start_year: str = None,
    *,
    refresh_table: bool = False,
) -> str:
    """Get an ID associated with a particular player name and source.

    Args:
        name (str): The name to lookup in the database.
        refresh_table (bool, optional): Whether to refresh the lookup table. Defaults
            to False.

    Raises:
        ValueError: If the source provided is not 'mlbam', 'fangraphs', 'bbref', or
            'retro'.
        NameNotFoundError: If the given name does not exist in the database.

    Returns:
        str: ID of the player associated with the given player name.
    """
    if source not in ["mlbam", "fangraphs", "bbref", "retro"]:
        raise ValueError(f"Invalid source: '{source}'")

    table = lookup_name(name, refresh_table=refresh_table)

    if len(table) > 1 and start_year:
        table = table.filter(pl.col("mlb_played_first") == start_year)

    elif len(table) > 1 and not start_year:
        table = table.sort("mlb_played_first")[-1]

    if table.is_empty():
        raise NameNotFoundError(name)

    return str(table[f"key_{source}"][0])
