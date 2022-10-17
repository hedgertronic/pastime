import io
from typing import cast

import pkg_resources
import polars as pl

from pastime.download import download_csv, PROGRESS_BAR


LOOKUP_URL = (
    "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"
)


LOOKUP_COLUMNS = (
    "name_full",
    "name_last",
    "name_first",
    "key_mlbam",
    "key_retro",
    "key_bbref",
    "key_bbref_minors",
    "key_fangraphs",
    "mlb_played_first",
    "mlb_played_last",
)


def get_lookup_table(refresh: bool = False) -> pl.DataFrame:
    if not pkg_resources.resource_exists(__name__, "data/lookup_table.csv") or refresh:
        output = io.StringIO()

        with PROGRESS_BAR:
            download_csv(
                LOOKUP_URL,
                output,
                PROGRESS_BAR,
                headers={"Accept-Encoding": "identity"},
            )

        columns = list(LOOKUP_COLUMNS)
        columns.pop(0)

        lookup_table = pl.read_csv(output, columns=columns).drop_nulls(
            subset=columns[2:]
        )

        # TODO: Fix when missing first name
        lookup_table = lookup_table.with_column(
            (pl.col("name_first") + " " + pl.col("name_last")).alias("name_full")
        )

        lookup_table = lookup_table[list(LOOKUP_COLUMNS)]
        lookup_table.write_csv("data/lookup_table.csv")
    else:
        stream = cast(
            io.BytesIO,
            pkg_resources.resource_stream(__name__, "data/lookup_table.csv"),
        )

        lookup_table = pl.read_csv(stream)

    return lookup_table


def lookup_id(player_id: int | str, *, refresh: bool = False) -> pl.DataFrame:
    data = get_lookup_table(refresh=refresh).filter(
        (pl.col("key_mlbam").cast(str) == str(player_id))
        | (pl.col("key_fangraphs").cast(str) == str(player_id))
        | (pl.col("key_bbref").cast(str) == str(player_id))
        | (pl.col("key_retro").cast(str) == str(player_id))
    )

    if data.is_empty():
        raise ValueError(
            f"Invalid player id: '{player_id}'."
            " If you think the lookup table may be out of date, you can refresh it by"
            " including 'refresh=True' in your function call."
        )

    return data


def lookup_name(
    name: str = "",
    *,
    refresh: bool = False,
) -> pl.DataFrame:
    lookup_table = get_lookup_table(refresh=refresh)

    name = name.strip().lower()

    data = lookup_table.filter(
        (pl.col("name_full").str.to_lowercase() == name)
        | (pl.col("name_first").str.to_lowercase() == name)
        | (pl.col("name_last").str.to_lowercase() == name)
    )

    if data.is_empty():
        raise ValueError(
            f"Invalid player name: '{name}'."
            " If you think the lookup table may be out of date, you can refresh it by"
            " including 'refresh=True' in your function call."
        )

    return data


def get_name(player_id: int | str, *, refresh: bool = False) -> str:
    table = lookup_id(player_id, refresh=refresh)

    if table.is_empty():
        raise ValueError(
            f"Invalid player id: '{player_id}'."
            " If you think the lookup table may be out of date, you can refresh it by"
            " including 'refresh=True' in your function call."
        )

    return table["name_full"][0]


def get_id(
    name: str, source: str = "mlbam", start_year: str = None, *, refresh: bool = False
) -> int:
    if source not in ["mlbam", "fangraphs", "bbref", "retro"]:
        raise ValueError(f"Invalid source: '{source}'")

    table = lookup_name(name, refresh=refresh)

    if len(table) > 1 and start_year:
        table = table.filter(pl.col("mlb_played_first") == start_year)

    elif len(table) > 1 and not start_year:
        table = table.sort("mlb_played_first")[-1]

    if table.is_empty():
        raise ValueError(
            f"Invalid player name: '{name}'."
            " If you think the lookup table may be out of date, you can refresh it by"
            " including 'refresh=True' in your function call."
        )

    return table[f"key_{source}"][0]
