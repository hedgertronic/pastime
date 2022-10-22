from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Sequence

import numpy as np
import pkg_resources

from pastime.exceptions import (
    FieldNameError,
    FieldTypeError,
    FieldValueError,
    RangeValidationError,
    TooManyValuesError,
)
from pastime.lookup import lookup_id
from pastime.type_aliases import Param, ParamComponent


@dataclass
class Field:
    name: str
    slug: str
    field_type: str
    choices: dict[str, str] = field(default_factory=dict)
    frequencies: dict[str, float] = field(default_factory=dict)
    aliases: dict[str, list[str]] = field(default_factory=dict)

    def get_params(self, values: Param) -> dict[str, list[str]]:
        return {self.slug: [str(self.validate_values(values))]}

    def get_values(self, params: dict[str, list[str]]) -> Param:
        return params[self.slug][0]

    def validate_values(self, values: Param) -> Any:
        return values

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        return self.frequencies.get("default", 1.0) if params.get(self.slug) else 1.0


class SingleSelectField(Field):
    def get_params(self, values: Param) -> dict[str, list[str]]:
        value = self.validate_values(values)
        param = self._get_param(value)

        return {self.slug: [param.replace(".", r"\.")]}

    def _get_param(self, value: str) -> str:
        param = ""

        if not value:
            pass

        elif value.lower() in self.choices:
            param = self.choices[value.lower()]

        elif value in self.choices.values():
            param = value

        else:
            raise FieldValueError(
                value=value, field_name=self.name, valid_values=self.choices.keys()
            )

        return param

    def get_values(self, params: dict[str, list[str]]) -> str:
        return params[self.slug][0].replace(r"\.", ".")

    def validate_values(self, values: Param) -> str:
        if (
            not isinstance(values, str)
            and isinstance(values, Sequence)
            and len(values) > 1
        ):
            raise TooManyValuesError(values=values, field_name=self.name, max_values=1)

        if not isinstance(values, str) and isinstance(values, Sequence):
            values = values[0]

        return str(values) if values else ""

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        return self.frequencies.get(
            self.get_values(params), self.frequencies.get("default", 1.0)
        )


@dataclass
class MultiSelectField(Field):
    delimiter: str = "|"
    add_trailing_delimiter: bool = True

    def get_params(self, values: Param) -> dict[str, list[str]]:
        valid_values = self.validate_values(values)

        params: set[str] = set()

        for value in valid_values:
            params |= self._get_param(value)

        param_list = [param.replace(".", r"\.") for param in sorted(params)]

        return {
            self.slug: [
                f"{self.delimiter.join(param_list)}"
                f"{self.delimiter if self.add_trailing_delimiter else ''}"
            ]
        }

    def _get_param(self, value: str) -> set[str]:
        params = set()

        if not value:
            pass

        elif value.lower() in self.choices:
            params.add(self.choices[value])

        elif value.lower() in self.aliases:
            params |= set(self.aliases[value])

        elif value in self.choices.values():
            params.add(value)

        else:
            raise FieldValueError(
                value=value, field_name=self.name, valid_values=self.choices.keys()
            )

        return params

    def get_values(self, params: dict[str, list[str]]) -> list[str]:
        return params[self.slug][0].replace(r"\.", ".").split("|")[:-1]

    def validate_values(self, values: Param) -> Sequence[str]:
        if not values:
            values = [""]

        elif isinstance(values, str | int | float | date):
            values = [str(values)]

        return [str(value) for value in values]

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        default_frequency = self.frequencies.get("default", 1.0)

        frequency_sum = sum(
            self.frequencies.get(value, default_frequency)
            for value in self.get_values(params)
        )

        return frequency_sum if frequency_sum <= 1.0 else 1.0


class PlayerLookupField(Field):
    def get_params(self, values: Param) -> dict[str, list[str]]:
        values = self.validate_values(values)

        return {self.slug: [str(value) for value in sorted(values) if value]}

    def get_values(self, params: dict[str, list[str]]) -> list[str]:
        return params[self.slug]

    def validate_values(self, values: Param) -> Sequence[str]:
        if not values:
            values = [""]

        elif isinstance(values, str | int | float | date):
            values = [str(values)]

        return [str(value) for value in values if not lookup_id(str(value)).is_empty()]

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        total = 0.01 * len(self.get_values(params))

        return total if total < 1.0 else 1.0


@dataclass
class DateField(Field):
    date_format: str = "%Y-%m-%d"
    min_value: int | None = None
    max_value: int | None = None

    def get_params(self, values: Param) -> dict[str, list[str]]:
        value = self.validate_values(values)

        return {self.slug: [value.strftime(self.date_format) if value else ""]}

    def get_values(self, params: dict[str, list[str]]) -> date | None:
        param = params.get(self.slug, [""])[0]

        return datetime.strptime(param, self.date_format).date() if param else None

    def validate_values(self, values: Param) -> date | None:
        if (
            not isinstance(values, str)
            and isinstance(values, Sequence)
            and len(values) > 1
        ):
            raise TooManyValuesError(values=values, field_name=self.name, max_values=1)

        if not isinstance(values, str) and isinstance(values, Sequence):
            values = values[0]

        if not values:
            values = None

        elif isinstance(values, str):
            values = datetime.strptime(values, self.date_format).date()

        elif isinstance(values, int | float):
            raise FieldTypeError(
                value=values, field_name=self.name, valid_types=[str, date, type(None)]
            )

        return values


@dataclass
class MetricRangeField(Field):
    min_value: int | None = None
    max_value: int | None = None

    def get_params(self, values: Param) -> dict[str, list[str]]:
        values = self.validate_values(values)

        range_min = self.min_value if self.min_value is not None else np.NINF
        range_max = self.max_value if self.max_value is not None else np.Inf

        min_value, max_value = self._get_range_extremes(values, range_min, range_max)

        return {
            "metric_{counter}": [self.slug],
            "metric_{counter}_gt": ([str(min_value) if min_value is not None else ""]),
            "metric_{counter}_lt": ([str(max_value) if max_value is not None else ""]),
        }

    def get_values(self, params: dict[str, list[str]]) -> tuple[str, str]:
        counter = 1

        while param := params.get(f"metric_{counter}", [""])[0]:
            if param == self.slug:
                return (
                    params[f"metric_{counter}_gt"][0],
                    params[f"metric_{counter}_lt"][0],
                )

            counter += 1

        return "", ""

    def validate_values(self, values: Param) -> tuple[float | None, float | None]:
        if not values and values != 0:
            values = [None, None]

        elif isinstance(values, str) or not isinstance(values, Sequence):
            value = self._validate_metric_value(values)
            values = [value, value]

        elif isinstance(values, Sequence) and len(values) == 0:
            values = [None, None]

        elif isinstance(values, Sequence) and len(values) > 2:
            raise TooManyValuesError(values=values, field_name=self.name, max_values=2)

        return (
            self._validate_metric_value(
                values[0] if values[0] or values[0] == 0 else None
            ),
            self._validate_metric_value(
                values[1] if values[1] or values[1] == 0 else None
            ),
        )

    def _validate_metric_value(self, value: ParamComponent) -> float | None:
        if not value and value != 0:
            value = None

        elif isinstance(value, date):
            raise FieldTypeError(
                value=value,
                field_name=self.name,
                valid_types=[int, float, type(None)],
            )

        elif isinstance(value, str | int):
            value = float(value)

        return value

    def _get_range_extremes(
        self,
        metric_values: tuple[float | None, float | None],
        range_min: int | float,
        range_max: int | float,
    ) -> tuple[float | None, float | None]:
        min_value = (
            np.clip(metric_values[0], range_min, range_max)
            if metric_values[0] is not None
            else None
        )

        max_value = (
            np.clip(metric_values[1], range_min, range_max)
            if metric_values[1] is not None
            else None
        )

        if min_value and max_value and float(min_value) > float(max_value):
            raise RangeValidationError(
                min_value=min_value, max_value=max_value, field_name=self.name
            )

        return min_value, max_value


# Want object with collection of fields associated
# SC Search: pitch_type, etc
# Each SC Leaderboard has its own fields
# Each FG Query has its own fields
# class Leaderboard:
#     def __init__(self, name: str, slug: str, fields: dict[str, dict[str, Any]]):
#         self.name = name
#         self.slug = slug
#         self.fields: dict[str, Field] = construct_fields(fields)

#     def get_params(self, values: dict[str, Param]) -> dict[str, list[str]]:
#         params: dict[str, list[str]] = {}

#         for field_name, value in values.items():
#             field_obj = self.fields.get(field_name)

#             if not field_obj:
#                 raise FieldNameError(
#                     field_name=field_name, valid_values=self.fields.keys()
#                 )

#             params |= field_obj.get_params(value)

#         return params


class Database:
    def __init__(self, name: str, slug: str, fields: dict[str, dict[str, Any]]):
        self.name = name
        self.slug = slug
        self.fields: dict[str, Field] = construct_fields(fields)

    def get_params(self, values: dict[str, Param]) -> dict[str, list[str]]:
        params: dict[str, list[str]] = {}

        for field_name, value in values.items():
            field_obj = self.fields.get(field_name)

            if not field_obj:
                raise FieldNameError(
                    field_name=field_name, valid_values=self.fields.keys()
                )

            params |= field_obj.get_params(value)

        return params


#######################################################################################
# FIELD DATA


FIELD_TYPES = {
    "single-select": SingleSelectField,
    "multi-select": MultiSelectField,
    "player-lookup": PlayerLookupField,
    "date-range": DateField,
    "metric-range": MetricRangeField,
}


def construct_fields(data: dict[str, dict[str, Any]]):
    return {
        field_name: FIELD_TYPES.get(field_data["field_type"], Field)(**field_data)
        for field_name, field_data in data.items()
    }


_statcast_data: dict[str, dict[str, Any]] = json.load(
    pkg_resources.resource_stream(__name__, "data/statcast_fields.json")
)


STATCAST_FIELDS: dict[str, Database] = {
    _db_name: Database(**_field_data)
    for _db_name, _field_data in _statcast_data.items()
}


_fangraphs_data: dict[str, dict[str, Any]] = json.load(
    pkg_resources.resource_stream(__name__, "data/fangraphs_fields.json")
)


FANGRAPHS_FIELDS: dict[str, Database] = {
    _db_name: Database(**_field_data)
    for _db_name, _field_data in _fangraphs_data.items()
}


# STATCAST_DEFAULT_PARAMS: dict[str, dict[str, list[str]]]


DEFAULT_SEARCH_PARAMS: dict[str, list[str]] = {}
# DEFAULT_LEADERBOARD_PARAMS: dict[str, dict[str, list[str]]] = {}


for _field in STATCAST_FIELDS["search"].fields.values():
    if _default_choice := _field.choices.get("default"):
        DEFAULT_SEARCH_PARAMS |= _field.get_params(_default_choice)

    # elif _field.field_type != "metric-range":
    #     DEFAULT_SEARCH_PARAMS |= {_field.slug: [""]}


# _search_field_data: dict[str, dict[str, Any]] = json.load(
#     pkg_resources.resource_stream(__name__, "data/search_fields.json")
# )

# _leaderboard_field_data: dict[str, dict[str, Any]] = json.load(
#     pkg_resources.resource_stream(__name__, "data/leaderboard_fields.json")
# )


# SEARCH_FIELDS: dict[str, Field] = construct_fields(_search_field_data)
# LEADERBOARD_FIELDS: dict[str, Leaderboard] = {
#     _leaderboard_name: Leaderboard(**_field_data)
#     for _leaderboard_name, _field_data in _leaderboard_field_data.items()
# }


# DEFAULT_SEARCH_PARAMS: dict[str, list[str]] = {}
# # DEFAULT_LEADERBOARD_PARAMS: dict[str, dict[str, list[str]]] = {}


# for _field in SEARCH_FIELDS.values():
#     if _default_choice := _field.choices.get("default"):
#         DEFAULT_SEARCH_PARAMS |= _field.get_params(_default_choice)

#     elif _field.field_type != "metric-range":
#         DEFAULT_SEARCH_PARAMS |= {_field.name: [""]}


# for _leaderboard in LEADERBOARD_FIELDS.values():
#     _default_params: dict[str, list[str]] = {}

#     for _field in _leaderboard.fields.values():
#         if _default_choice := _field.choices.get("default"):
#             _default_params |= _field.get_params(_default_choice)

#         elif _field.field_type != "metric-range":
#             _default_params |= {_field.slug: [""]}

#     DEFAULT_LEADERBOARD_PARAMS[_leaderboard.name] = _default_params
