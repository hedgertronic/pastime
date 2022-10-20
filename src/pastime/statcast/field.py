from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Sequence

import numpy as np

from pastime.lookup import lookup_id
from pastime.statcast.exceptions import (
    FieldNameError,
    FieldTypeError,
    FieldValueError,
    RangeValidationError,
    TooManyValuesError,
)


ParamComponent = str | int | float | date | None
Param = ParamComponent | Sequence[ParamComponent]


@dataclass
class Field:
    name: str
    slug: str
    field_type: str
    choices: dict[str, str] = field(default_factory=dict)
    frequencies: dict[str, float] = field(default_factory=dict)
    aliases: dict[str, list[str]] = field(default_factory=dict)
    delimiter: str = "|"
    add_trailing_delimiter: bool = True
    min_value: int | None = None
    max_value: int | None = None

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


class MultiSelectField(Field):
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


class DateRangeField(Field):
    def get_params(self, values: Param) -> dict[str, list[str]]:
        start, end = self.validate_values(values)

        if start and end and start > end:
            raise RangeValidationError(
                min_value=str(start), max_value=str(end), field_name=self.name
            )

        return {
            f"{self.slug}_gt": [str(start) if start else ""],
            f"{self.slug}_lt": [str(end) if end else ""],
        }

    def get_values(
        self, params: dict[str, list[str]]
    ) -> tuple[date | None, date | None]:
        start_date = params.get(f"{self.slug}_gt", [""])[0]
        end_date = params.get(f"{self.slug}_lt", [""])[0]

        return (
            date.fromisoformat(start_date) if start_date else None,
            date.fromisoformat(end_date) if end_date else None,
        )

    def validate_values(self, values: Param) -> tuple[date | None, date | None]:
        if not values:
            values = [None, None]

        elif isinstance(values, str) or not isinstance(values, Sequence):
            value = self._validate_date_value(values)
            values = [value, value]

        elif isinstance(values, Sequence) and len(values) == 0:
            values = [None, None]

        elif isinstance(values, Sequence) and len(values) > 2:
            raise TooManyValuesError(values=values, field_name=self.name, max_values=2)

        return (
            self._validate_date_value(values[0]) if values[0] else None,
            self._validate_date_value(values[1]) if values[1] else None,
        )

    def _validate_date_value(self, value: ParamComponent) -> date | None:
        if not value:
            value = None

        elif isinstance(value, str):
            value = date.fromisoformat(value)

        elif isinstance(value, int | float):
            raise FieldTypeError(
                value=value, field_name=self.name, valid_types=[str, date, type(None)]
            )

        return value


class MetricRangeField(Field):
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


class Leaderboard:
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


FIELD_TYPES = {
    "single-select": SingleSelectField,
    "multi-select": MultiSelectField,
    "player-lookup": PlayerLookupField,
    "date-range": DateRangeField,
    "metric-range": MetricRangeField,
}


def construct_fields(data: dict[str, dict[str, Any]]):
    return {
        field_name: FIELD_TYPES.get(field_data["field_type"], Field)(**field_data)
        for field_name, field_data in data.items()
    }
