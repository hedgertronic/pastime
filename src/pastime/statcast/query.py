import io
import json
from datetime import date, timedelta
from difflib import get_close_matches
from typing import Any, cast

import pkg_resources
import polars as pl

from pastime.download import download_file, download_files
from pastime.statcast.analysis import spin_columns
from pastime.statcast.field import (
    Field,
    Leaderboard,
    MetricRangeField,
    Param,
    construct_fields,
)


#######################################################################################
# FIELD DATA


_search_field_data: dict[str, dict[str, Any]] = json.load(
    pkg_resources.resource_stream(__name__, "data/search_fields.json")
)

_leaderboard_field_data: dict[str, dict[str, Any]] = json.load(
    pkg_resources.resource_stream(__name__, "data/leaderboard_fields.json")
)


SEARCH_FIELDS: dict[str, Field] = construct_fields(_search_field_data)
LEADERBOARD_FIELDS: dict[str, Leaderboard] = {
    _leaderboard_name: Leaderboard(**_field_data)
    for _leaderboard_name, _field_data in _leaderboard_field_data.items()
}


DEFAULT_SEARCH_PARAMS: dict[str, list[str]] = {}
# DEFAULT_LEADERBOARD_PARAMS: dict[str, dict[str, list[str]]] = {}


for _field in SEARCH_FIELDS.values():
    if _default_choice := cast(str, _field.choices.get("default")):
        DEFAULT_SEARCH_PARAMS |= _field.get_params(_default_choice)

    elif _field.field_type != "metric-range":
        DEFAULT_SEARCH_PARAMS |= {_field.name: [""]}


# for _leaderboard in LEADERBOARD_FIELDS.values():
#     _default_params: dict[str, list[str]] = {}

#     for _field in _leaderboard.fields.values():
#         if _default_choice := cast(str, _field.choices.get("default")):
#             _default_params |= _field.get_params(_default_choice)

#         elif _field.field_type != "metric-range":
#             _default_params |= {_field.slug: [""]}

#     DEFAULT_LEADERBOARD_PARAMS[_leaderboard.name] = _default_params


#######################################################################################
# CONSTANTS FOR SPLITTING REQUESTS


MAX_ROWS_PER_REQUEST = 15_000


GAMES_PER_REGULAR_SEASON = 2_430
GAMES_PER_POSTSEASON = 53
GAMES_PER_YEAR = GAMES_PER_REGULAR_SEASON + GAMES_PER_POSTSEASON


PITCHES_PER_GAME = 325
PITCHES_PER_YEAR = GAMES_PER_YEAR * PITCHES_PER_GAME


#######################################################################################
# OTHER USEFUL INFO


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


SEASON_DATES = {
    2008: {"start": date(2008, 3, 25), "end": date(2008, 10, 27)},
    2009: {"start": date(2009, 4, 5), "end": date(2009, 11, 4)},
    2010: {"start": date(2010, 4, 4), "end": date(2010, 11, 1)},
    2011: {"start": date(2011, 3, 31), "end": date(2011, 10, 28)},
    2012: {"start": date(2012, 3, 28), "end": date(2012, 10, 28)},
    2013: {"start": date(2013, 3, 31), "end": date(2013, 10, 30)},
    2014: {"start": date(2014, 3, 22), "end": date(2014, 10, 29)},
    2015: {"start": date(2015, 4, 5), "end": date(2015, 11, 1)},
    2016: {"start": date(2016, 4, 3), "end": date(2016, 11, 2)},
    2017: {"start": date(2017, 4, 2), "end": date(2017, 11, 1)},
    2018: {"start": date(2018, 3, 29), "end": date(2018, 10, 28)},
    2019: {"start": date(2019, 3, 20), "end": date(2019, 10, 30)},
    2020: {"start": date(2020, 7, 23), "end": date(2020, 10, 27)},
    2021: {"start": date(2021, 4, 1), "end": date(2021, 11, 2)},
    2022: {"start": date(2022, 4, 7), "end": date(2022, 11, 5)},
}


_SWING_TAKE_GROUPS = {
    "Swing-Take": ["swing", "take"],
    "Pitch Type": ["fastball", "curve", "cutter", "changeup", "slider", "other"],
    "Attack Region": ["heart", "shadow", "chase", "waste"],
    "Bat-side": ["R", "L"],
}


#######################################################################################
# URLS


SEARCH_URL = "https://baseballsavant.mlb.com/statcast_search/csv?"
LEADERBOARD_URL = "https://baseballsavant.mlb.com/leaderboard"


#######################################################################################
# CLASSES


class SearchQuery:
    ####################################################################################
    # PUBLIC METHODS

    def __init__(self, **kwargs: Param):
        self.params = DEFAULT_SEARCH_PARAMS
        self.frequency = 1.0
        self.metric_counter = 1
        self.requests_to_make: list[dict[str, list[str]]] = []

        date_range = kwargs.pop("date_range", None)

        for field_name, field_values in kwargs.items():
            if not field_values:
                continue

            field = SEARCH_FIELDS.get(field_name)

            if not field:
                close_matches = get_close_matches(field_name, SEARCH_FIELDS.keys())

                raise ValueError(
                    f"Invalid field name: {field_name}."
                    f" Did you mean {', '.join(close_matches)}?"
                    if close_matches
                    else f"Invalid field name: {field_name}."
                )

            self._add_param(field, field_values)

        self._add_dates(date_range or None)

    def update_seasons(self):
        season_field = SEARCH_FIELDS["season"]

        seasons = cast(list[str], season_field.get_values(self.params))

        start, end = cast(
            tuple[date | None, date | None],
            SEARCH_FIELDS["date_range"].get_values(self.params),
        )

        self._add_param(
            season_field,
            list(
                range(
                    start.year if start else int(seasons[0]),
                    end.year + 1 if end else int(seasons[-1]),
                )
            ),
        )

    def request(self, add_spin_columns: bool = False, **kwargs) -> pl.DataFrame:
        self._prepare_requests()

        output = io.StringIO()

        output = download_files(
            url=SEARCH_URL,
            output=output,
            params=self.requests_to_make,
            request_name="Statcast Search",
            messages=self._get_messages(),
            **kwargs,
        )

        data = (
            pl.read_csv(output, parse_dates=True, ignore_errors=True)
            .drop_nulls(subset="game_date")
            .drop(DEPRECATED_COLUMNS)
            .sort(["game_date", "game_pk", "at_bat_number", "pitch_number"])
        )

        return spin_columns(data) if add_spin_columns else data

    ####################################################################################
    # HELPER METHODS

    def _add_param(self, field: Field, values: Param) -> None:
        if not values:
            return

        new_params = field.get_params(values)

        if isinstance(field, MetricRangeField):
            new_params[f"metric_{self.metric_counter}"] = new_params.pop(
                "metric_{counter}"
            )
            new_params[f"metric_{self.metric_counter}_gt"] = new_params.pop(
                "metric_{counter}_gt"
            )
            new_params[f"metric_{self.metric_counter}_lt"] = new_params.pop(
                "metric_{counter}_lt"
            )

            self.metric_counter += 1

        self.params |= new_params
        self.frequency *= field.get_frequency(self.params)

    def _add_dates(self, date_range: Param) -> None:
        date_range_field = SEARCH_FIELDS["date_range"]
        date_params = date_range_field.get_params(date_range)

        seasons = cast(list[str], SEARCH_FIELDS["season"].get_values(self.params))

        start = date_params["game_date_gt"][0] or SEASON_DATES[int(seasons[0])]["start"]
        end = date_params["game_date_lt"][0] or SEASON_DATES[int(seasons[-1])]["end"]

        self._add_param(date_range_field, [start, end])

    def _prepare_requests(self):
        request_date_pairs = self._get_date_pairs()

        for start_date, end_date in request_date_pairs:
            params_copy = self.params.copy()

            params_copy["game_date_gt"] = [start_date]
            params_copy["game_date_lt"] = [end_date]

            self.requests_to_make.append(params_copy)

    def _get_date_pairs(self) -> list[tuple[str, str]]:
        date_pairs: list[tuple[str, str]] = []

        start, end = cast(
            tuple[date, date],
            SEARCH_FIELDS["date_range"].get_values(self.params),
        )

        est_rows = PITCHES_PER_YEAR * self.frequency

        if est_rows < MAX_ROWS_PER_REQUEST:
            return [(str(start), str(end))]

        for season in range(start.year, end.year + 1):
            season_start = SEASON_DATES[season]["start"]
            season_end = SEASON_DATES[season]["end"]

            range_start = max(season_start, start)

            while range_start <= min(season_end, end):
                range_end = min(
                    end, range_start + timedelta(days=int(2 / self.frequency))
                )

                date_pairs.append(
                    (
                        f"{season}-01-01"
                        if range_start == season_start
                        else str(range_start),
                        str(range_end),
                    )
                )

                range_start = range_end + timedelta(days=1)

        return date_pairs

    def _get_messages(self) -> list[str]:
        messages = []

        start, end = cast(
            tuple[date | None, date | None],
            SEARCH_FIELDS["date_range"].get_values(self.params),
        )

        if start and start < date(2008, 1, 1):
            messages.append(
                "Statcast data is only available from the 2008 season onwards."
            )

        if start and start < date(2015, 1, 1):
            messages.append(
                "Some metrics such as 'exit velocity' and 'batted ball events'"
                " are not available before 2015."
            )

        if end and end >= date.today():
            messages.append(
                "Data is updated every day at 3 am."
                " Some of today's data may be missing."
            )

        return messages


class LeaderboardQuery:
    ####################################################################################
    # PUBLIC METHODS
    def __init__(self, name: str, **kwargs: Param):
        leaderboard = LEADERBOARD_FIELDS.get(name)

        if not leaderboard:
            close_matches = get_close_matches(name, LEADERBOARD_FIELDS.keys())

            raise ValueError(
                f"Invalid leaderboard name: '{name}'."
                f" Did you mean {', '.join(close_matches)}?"
                if close_matches
                else f"Invalid field name: '{name}'."
            )

        self.leaderboard = leaderboard
        self.params: dict[str, list[str]] = {}

        for field_name, field_values in kwargs.items():
            if not field_values:
                continue

            field = self.leaderboard.fields.get(field_name)

            if not field:
                close_matches = get_close_matches(
                    field_name, self.leaderboard.fields.keys()
                )

                raise ValueError(
                    f"Invalid field name: '{field_name}'."
                    f" Did you mean {', '.join(close_matches)}?"
                    if close_matches
                    else f"Invalid field name: '{field_name}'."
                )

            self.params |= field.get_params(field_values)

        if leaderboard.name == "swing_take":
            group = self.params["type"][0]
            subgroup = self.params.get("sub_type", [""])[0]

            if group == "All" and subgroup:
                raise ValueError(
                    f"Invalid value provided: '{subgroup}' for group 'All'."
                    " When group is All, subgroup must be None."
                )

            if subgroup and subgroup.lower() not in _SWING_TAKE_GROUPS[group]:
                raise ValueError(
                    f"Invalid value provided: '{subgroup}'"
                    f" for group '{group}'."
                    f" Values for '{group}' must be in"
                    f" {_SWING_TAKE_GROUPS[group]}"
                )

        self.params["csv"] = ["true"]

    def request(self, **kwargs) -> pl.DataFrame:
        output = io.StringIO()

        output = download_file(
            url=f"{LEADERBOARD_URL}/{self.leaderboard.slug}",
            output=output,
            params=self.params,
            request_name="Statcast Leaderboard",
            **kwargs,
        )

        return pl.read_csv(output, parse_dates=True, ignore_errors=True)
