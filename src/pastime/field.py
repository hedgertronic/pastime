"""Store data for fields used for making query requests.

This module provides classes that simplify interacting with field data.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, NamedTuple, Sequence, Type

import numpy as np
import pkg_resources

from pastime.exceptions import (
    FieldTypeError,
    FieldValueError,
    InvalidBoundError,
    LessThanLowerBoundError,
    MoreThanUpperBoundError,
    TooManyValuesError,
)
from pastime.lookup import lookup_id
from pastime.type_aliases import Param, ParamComponent


#######################################################################################
# FIELD CLASSES


@dataclass
class Field:
    """A base class for a field.

    Attributes:
        name (str): Name of the field.
        slug (str): URL slug of the field.
        field_type (str): Type of the field.
        choices (dict[str, str], optional): Choices that are valid for the field.
        frequencies (dict[str, float], optional): Estimated frequencies for choices
            used to break up requests if needed. A base Field class only contains a
            default frequency.
    """

    name: str
    slug: str
    field_type: str
    choices: dict[str, str] = field(default_factory=dict)
    frequencies: dict[str, float] = field(default_factory=dict)

    def get_params(self, values: Param) -> dict[str, list[str]]:
        """Create dict of params that can be urlencoded in a Requests request.

        Args:
            values (Param): A list of values to put into a query string.

        Returns:
            dict[str, list[str]]: Dict mapping the slug of the field to a query string
                of validated values.
        """
        return {self.slug: [str(self._validate_values(values))]}

    def get_values(self, params: dict[str, list[str]]) -> Param:
        """Get values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            Param: Values that are mapped to the field's slug parsed from the dict of
                query strings.
        """
        return params[self.slug][0]

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        """Get frequency of values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            float: Total frequency of values that are mapped to the field's slug from
                the dict of params.
        """
        return self.frequencies.get("default", 1.0) if params.get(self.slug) else 1.0

    def _validate_values(self, values: Param) -> Param:
        """Confirm the type and structure of a given value."""
        return values


class SingleSelectField(Field):
    """A field that accepts a single value.

    Attributes:
        name (str): Name of the field.
        slug (str): URL slug of the field.
        field_type (str): Type of the field.
        choices (dict[str, str], optional): Choices that are valid for the field.
        frequencies (dict[str, float], optional): Estimated frequencies for choices
            used to break up requests if needed.

    Raises:
        FieldValueError: If a given value is not a valid choice or alias.
        TooManyValuesError: If more than one value is provided.
    """

    def get_params(self, values: Param) -> dict[str, list[str]]:
        """Create dict of params that can be urlencoded in a Requests request.

        A SingleSelectField only accepts one value. This value can be either the name
        or the URL slug of a given choice. Some URL slugs are confusing and unclear
        (e.g. the URL slugs for venues in the Statcast Search collection are integers
        with no obvious connection to the venue they are supposed to represent). On the
        other hand, some URL slugs can be easier and quicker to use (the URL slugs for
        pitch types are only two characters long -- "FF" is easier to type than "4-seam
        fastball").

        Args:
            values (Param): A value to put into a query string.

        Returns:
            dict[str, list[str]]: Dict mapping the slug of the field to a query
                string of a validated value.
        """
        value = self._validate_values(values)
        param = self._get_param(value)

        return {self.slug: [param.replace(".", r"\.")]}

    def _get_param(self, value: str) -> str:
        """Confirm whether a value is a valid choice or alias."""
        param = ""

        if not value:
            pass

        # Each key of the choices instance variable represents an easy to understand
        # name of the respective choice.

        elif value.lower() in self.choices:
            param = self.choices[value.lower()]

        # Each value of the choices instance variables represents the slug of the
        # respective choice.

        elif value in self.choices.values():
            param = value

        else:
            raise FieldValueError(
                value=value, field_name=self.name, valid_values=self.choices.keys()
            )

        return param

    def get_values(self, params: dict[str, list[str]]) -> str:
        """Get value from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            Param: Value that is mapped to the field's slug parsed from the dict of
                query strings.
        """
        return params[self.slug][0].replace(r"\.", ".")

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        """Get frequency of values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            float: Total frequency of values that are mapped to the field's slug from
                the dict of params.
        """
        return self.frequencies.get(
            self.get_values(params), self.frequencies.get("default", 1.0)
        )

    def _validate_values(self, values: Param) -> str:
        """Confirm the type and structure of a given value."""
        if not values:
            values = ""

        elif (
            not isinstance(values, str)
            and isinstance(values, Sequence)
            and len(values) > 1
        ):
            raise TooManyValuesError(values=values, field_name=self.name, max_values=1)

        elif not isinstance(values, str) and isinstance(values, Sequence):
            values = values[0]

        return str(values)


@dataclass
class MultiSelectField(Field):
    """A field that accepts multiple values.

    Attributes:
        name (str): Name of the field.
        slug (str): URL slug of the field.
        field_type (str): Type of the field.
        choices (dict[str, str], optional): Choices that are valid for the field.
        frequencies (dict[str, float], optional): Estimated frequencies for choices
            used to break up requests if needed.
        aliases (dict[str, list[str]], optional): A list of aliases that represent
            multiple choices, used for grouping together choices that intuitively
            go together.
        delimiter (str): Character used to separate choices in query string. Defaults
            to a pipe, the Statcast search standard.
        add_trailing_delimter (bool): Whether an extra delimiter should be placed at
            the end of the query string. Defaults to True, the Statcast search
            standard.

    Raises:
        FieldValueError: If a given value is not a valid choice or alias.
    """

    aliases: dict[str, list[str]] = field(default_factory=dict)
    delimiter: str = "|"
    add_trailing_delimiter: bool = True

    def get_params(self, values: Param) -> dict[str, list[str]]:
        """Create dict of params that can be urlencoded in a Requests request.

        A MultiSelectField accepts multiple values. These values can be either the name
        or the URL slug of a given choice. Some URL slugs are confusing and unclear
        (e.g. the URL slugs for venues in the Statcast Search collection are integers
        with no obvious connection to the venue they are supposed to represent). On the
        other hand, some URL slugs can be easier and quicker to use (the URL slugs for
        pitch types are only two characters long -- "FF" is easier to type than "4-seam
        fastball").

        Args:
            values (Param): A list of values to put into a query string.

        Returns:
            dict[str, list[str]]: Dict mapping the slug of the field to a query string
                of validated values.
        """
        valid_values = self._validate_values(values)

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
        """Confirm whether a value is a valid choice or alias."""
        params = set()

        if not value:
            pass

        # Each key of the choices instance variable represents an easy to understand
        # name of the respective choice.

        elif value.lower() in self.choices:
            params.add(self.choices[value])

        elif value.lower() in self.aliases:
            params |= set(self.aliases[value])

        # Each value of the choices instance variables represents the slug of the
        # respective choice.

        elif value in self.choices.values():
            params.add(value)

        else:
            raise FieldValueError(
                value=value, field_name=self.name, valid_values=self.choices.keys()
            )

        return params

    def get_values(self, params: dict[str, list[str]]) -> list[str]:
        """Get values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            Param: Values that are mapped to the field's slug parsed from the dict of
                query strings.
        """
        values = params[self.slug][0].replace(r"\.", ".").split(self.delimiter)

        return values[:-1] if self.add_trailing_delimiter else values

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        """Get frequency of values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            float: Total frequency of values that are mapped to the field's slug from
                the dict of params.
        """
        default_frequency = self.frequencies.get("default", 1.0)

        frequency_sum = sum(
            self.frequencies.get(value, default_frequency)
            for value in self.get_values(params)
        )

        return frequency_sum if frequency_sum <= 1.0 else 1.0

    def _validate_values(self, values: Param) -> Sequence[str]:
        """Confirm the type and structure of a given value."""
        if not values:
            values = [""]

        elif isinstance(values, str | int | float | date):
            values = [str(values)]

        return [str(value) for value in values]


class PlayerField(Field):
    """A field that accepts player ID values.

    Attributes:
        name (str): Name of the field.
        slug (str): URL slug of the field.
        field_type (str): Type of the field.
        choices (dict[str, str], optional): Choices that are valid for the field. This
            will always be empty for this field.
        frequencies (dict[str, float], optional): Estimated frequencies for choices
            used to break up requests if needed. This will always be empty for this
            field.

    Raises:
        IdNotFoundError: If a given value is not a valid player ID.
    """

    def get_params(self, values: Param) -> dict[str, list[str]]:
        """Create dict of params that can be urlencoded in a Requests request.

        A PlayerField accepts multiple values.

        Args:
            values (Param): A list of values to put into a query string.

        Returns:
            dict[str, list[str]]: Dict mapping the slug of the field to a query string
                of validated player ID values.
        """
        values = self._validate_values(values)

        return {self.slug: [str(value) for value in sorted(values) if value]}

    def get_values(self, params: dict[str, list[str]]) -> list[str]:
        """Get values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            Param: Values that are mapped to the field's slug parsed from the dict of
                query strings.
        """
        return params[self.slug]

    def get_frequency(self, params: dict[str, list[str]]) -> float:
        """Get frequency of values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            float: Total frequency of values that are mapped to the field's slug from
                the dict of params.
        """
        total = 0.01 * len(self.get_values(params))

        return total if total < 1.0 else 1.0

    def _validate_values(self, values: Param) -> Sequence[str]:
        """Confirm the type and structure of a given value."""
        if not values:
            values = [""]

        elif isinstance(values, str | int | float | date):
            values = [str(values)]

        return [str(value) for value in values if not lookup_id(str(value)).is_empty()]


@dataclass
class DateField(Field):
    """A field that accepts date values.

    Attributes:
        name (str): Name of the field.
        slug (str): URL slug of the field.
        field_type (str): Type of the field.
        choices (dict[str, str], optional): Choices that are valid for the field. This
            will always be empty for this field.
        frequencies (dict[str, float], optional): Estimated frequencies for choices
            used to break up requests if needed. This will always be empty for this
            field.
        date_format (str, optional): Format to use to parse dates. Default to ISO 8601
            format (`YYYY-MM-DD`).
        min_value (str, optional): Minimum date value to accept.
        max_value (str, optional): Maximum date value to accept.
        all_dates_slug (str, optional): Slug to use when all dates are chosen.

    Raises:
        FieldTypeError: If a given value is not a date or date string.
        TooManyValuesError: If more than one value is provided.
        LessThanLowerBoundError: If a given value is less than the field's min value.
        MoreThanUpperBoundError: If a given value is more than the field's max value.
        ValueError: If a given value does not parse according to the date format.
    """

    date_format: str = "%Y-%m-%d"
    min_value: str | None = None
    max_value: str | None = None
    all_dates_slug: str | None = None

    def get_params(self, values: Param) -> dict[str, list[str]]:
        """Create dict of params that can be urlencoded in a Requests request.

        A DateField accepts multiple values.

        Args:
            values (Param): A value to put into a query string.

        Returns:
            dict[str, list[str]]: Dict mapping the slug of the field to a query string
                of a validated date value.
        """
        value = self._validate_values(values)

        return {self.slug: [value.strftime(self.date_format) if value else ""]}

    def get_values(self, params: dict[str, list[str]]) -> date | None:
        """Get values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            Param: Values that are mapped to the field's slug parsed from the dict of
                query strings.
        """
        param = params.get(self.slug, [""])[0]

        return datetime.strptime(param, self.date_format).date() if param else None

    def _validate_values(self, values: Param) -> date | None:
        """Confirm the type and structure of a given value."""
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

        elif isinstance(values, float):
            raise FieldTypeError(
                value=values, field_name=self.name, valid_types=[str, date, type(None)]
            )

        elif isinstance(values, str | int):
            values = datetime.strptime(str(values), self.date_format).date()

        return self._validate_date(values)

    def _validate_date(self, values: date | None) -> date | None:
        """Confirm the value of a given date value."""
        min_value = (
            datetime.strptime(str(self.min_value), self.date_format).date()
            if self.min_value
            else None
        )

        max_value = (
            datetime.strptime(str(self.max_value), self.date_format).date()
            if self.max_value
            else None
        )

        if min_value and values and values < min_value:
            raise LessThanLowerBoundError(
                values.strftime(self.date_format), min_value, self.name
            )

        if max_value and values and values > max_value:
            raise MoreThanUpperBoundError(
                values.strftime(self.date_format), max_value, self.name
            )

        return values


@dataclass
class MetricField(Field):
    """A field that accepts a lower and upper bound value.

    Attributes:
        name (str): Name of the field.
        slug (str): URL slug of the field.
        field_type (str): Type of the field.
        choices (dict[str, str], optional): Choices that are valid for the field. This
            will always be empty for this field.
        frequencies (dict[str, float], optional): Estimated frequencies for choices
            used to break up requests if needed. This will always be empty for this
            field.
        min_value (int, optional): Minimum metric value to accept.
        max_value (int, optional): Maximum metric value to accept.

    Raises:
        FieldTypeError: If a given value is not an int, float, or a string that can be
            parsed to an int or float.
        TooManyValuesError: If more than two values are provided.
        LessThanLowerBoundError: If a given value is less than the field's min value.
        MoreThanUpperBoundError: If a given value is more than the field's max value.
        InvalidBoundError: If the lower bound provided is higher than the upper bound
            provided.
    """

    min_value: int | None = None
    max_value: int | None = None

    def get_params(self, values: Param) -> dict[str, list[str]]:
        """Create dict of params that can be urlencoded in a Requests request.

        A MetricField accepts sequences of two items that represent a lower and upper
        bound of the metric. A singular value can be provided as well, in which case
        that value will be used as both the lower and upper bound.

        Args:
            values (Param): One or two items to be put into a query string.

        Returns:
            dict[str, list[str]]: Dict mapping the slug of the field to a query string
                of a validated metric value, along with additional slugs and query
                strings for the lower and upper board of the metric.
        """
        values = self._validate_values(values)
        min_value, max_value = self._get_range_extremes(values)

        return {
            "metric": [self.slug],
            "metric_gt": ([str(min_value) if min_value is not None else ""]),
            "metric_lt": ([str(max_value) if max_value is not None else ""]),
        }

    def get_values(self, params: dict[str, list[str]]) -> tuple[str, str]:
        """Get values from a dict of params.

        Args:
            params (dict[str, list[str]]): A dict of URL slugs to query strings.

        Returns:
            Param: Values that are mapped to the field's slug parsed from the dict of
                query strings.
        """
        counter = 1

        while param := params.get(f"metric_{counter}", [""])[0]:
            if param == self.slug:
                return (
                    params[f"metric_{counter}_gt"][0],
                    params[f"metric_{counter}_lt"][0],
                )

            counter += 1

        return "", ""

    def _validate_values(self, values: Param) -> tuple[float | None, float | None]:
        """Confirm the type and structure of a given value."""
        if not values and values != 0:
            values = [None, None]

        elif isinstance(values, str) or not isinstance(values, Sequence):
            value = self._validate_metric(values)
            values = [value, value]

        elif isinstance(values, Sequence) and len(values) == 0:
            values = [None, None]

        elif isinstance(values, Sequence) and len(values) > 2:
            raise TooManyValuesError(values=values, field_name=self.name, max_values=2)

        return (self._validate_metric(values[0]), self._validate_metric(values[1]))

    def _validate_metric(self, value: ParamComponent) -> float | None:
        """Confirm the type and value of a given metric value."""
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
    ) -> tuple[float | None, float | None]:
        """Get the min and max value from a given range."""
        min_value = metric_values[0]
        max_value = metric_values[1]

        range_min = self.min_value if self.min_value is not None else np.NINF
        range_max = self.max_value if self.max_value is not None else np.Inf

        if min_value and min_value < range_min:
            raise LessThanLowerBoundError(min_value, range_min, self.name)

        if max_value and max_value > range_max:
            raise MoreThanUpperBoundError(max_value, range_max, self.name)

        if min_value and max_value and float(min_value) > float(max_value):
            raise InvalidBoundError(
                min_value=min_value, max_value=max_value, field_name=self.name
            )

        return min_value, max_value


class Collection(NamedTuple):
    """A collection of fields that are all related to the same query.

    Args:
        name (str): Name of the collection.
        slug (str): URL slug of the collection.
        fields (dict[str, Field]): Mapping of field names to Field objects.
    """

    name: str
    slug: str
    fields: dict[str, Field]


#######################################################################################
# FIELD CONSTRUCTION


# A mapping of field names to the actual field classes for easier construction
FIELD_TYPES: dict[str, Type[Field]] = {
    "single-select": SingleSelectField,
    "multi-select": MultiSelectField,
    "player-lookup": PlayerField,
    "date-range": DateField,
    "metric-range": MetricField,
}


def construct_fields(data: dict[str, dict[str, Any]]) -> dict[str, Field]:
    """Construct Field objects from JSON data.

    Args:
        data (dict[str, dict[str, Any]]): Raw data describing the arguments of a Field
            object.

    Returns:
        dict[str, Field]: A dictionary of Field objects mapped to their name.
    """
    return {
        field_name: FIELD_TYPES.get(field_data["field_type"], Field)(**field_data)
        for field_name, field_data in data.items()
    }


#######################################################################################
# MANAGING FIELD FILES


_statcast_data: dict[str, dict[str, Any]] = json.load(
    pkg_resources.resource_stream(__name__, "data/statcast_fields.json")
)


_fangraphs_data: dict[str, dict[str, Any]] = json.load(
    pkg_resources.resource_stream(__name__, "data/fangraphs_fields.json")
)


#######################################################################################
# CREATING FIELD OBJECTS


# A mapping of collection names to their respective Statcast field collections
STATCAST_FIELDS: dict[str, Collection] = {
    _collection_name: Collection(
        _field_data["name"],
        _field_data["slug"],
        construct_fields(_field_data["fields"]),
    )
    for _collection_name, _field_data in _statcast_data.items()
}


# A mapping of collection names to their respective Fangraphs field collections
FANGRAPHS_FIELDS: dict[str, Collection] = {
    _collection_name: Collection(
        _field_data["name"],
        _field_data["slug"],
        construct_fields(_field_data["fields"]),
    )
    for _collection_name, _field_data in _fangraphs_data.items()
}
