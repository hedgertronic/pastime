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
    if (
        not pkg_resources.resource_exists(__name__, "data/lookup_table.csv")
        or refresh_table
    ):
        output = io.StringIO()

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
    name: str = "",
    *,
    refresh_table: bool = False,
) -> pl.DataFrame:
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
) -> int:
    if source not in ["mlbam", "fangraphs", "bbref", "retro"]:
        raise ValueError(f"Invalid source: '{source}'")

    table = lookup_name(name, refresh_table=refresh_table)

    if len(table) > 1 and start_year:
        table = table.filter(pl.col("mlb_played_first") == start_year)

    elif len(table) > 1 and not start_year:
        table = table.sort("mlb_played_first")[-1]

    if table.is_empty():
        raise NameNotFoundError(name)

    return table[f"key_{source}"][0]
