from datetime import date
from difflib import get_close_matches
from typing import Iterable, Sequence, Type


class FieldNameError(ValueError):
    def __init__(self, field_name: str, valid_values: Iterable[str]):
        message = f"'{field_name}'"

        close_matches = get_close_matches(field_name, valid_values)

        if close_matches:
            close_matches_in_quotes = [f"'{cm}'" for cm in close_matches]
            message += f"; did you mean {', '.join(close_matches_in_quotes)}?"

        super().__init__(message)


class FieldValueError(ValueError):
    def __init__(self, value: str, field_name: str, valid_values: Iterable[str]):
        message = f"'{value}' for field '{field_name}'"

        close_matches = get_close_matches(value, valid_values)

        if close_matches:
            close_matches_in_quotes = [f"'{cm}'" for cm in close_matches]
            message += f"; did you mean {', '.join(close_matches_in_quotes)}?"

        super().__init__(message)


class FieldTypeError(TypeError):
    def __init__(
        self,
        value: str | int | float | date | None,
        field_name: str,
        valid_types: Sequence[Type],
    ):
        message = (
            f"{type(value)} for field '{field_name}'"
            f"; expected {', '.join(str(vt) for vt in valid_types)}"
        )

        super().__init__(message)


class TooManyValuesError(TypeError):
    def __init__(
        self,
        values: Sequence[str | int | float | date | None],
        field_name: str,
        max_values: int,
    ):
        message = (
            f"'{values}'"
            f"; field '{field_name}' takes a maximum of {max_values} values"
        )

        super().__init__(message)


class RangeValidationError(ValueError):
    def __init__(
        self,
        min_value: str | int | float,
        max_value: str | int | float,
        field_name: str = None,
    ):
        message = f"Min value '{min_value}' > max value '{max_value}'"

        if field_name:
            message += f" for field '{field_name}'"

        super().__init__(message)


class InvalidSubgroupError(ValueError):
    def __init__(
        self,
        group: str,
        subgroup: str,
        leaderboard: str,
        valid_values: Sequence[str] | None,
    ):
        message = (
            f"'{subgroup}'"
            f" for group '{group}' in leaderboard '{leaderboard}'"
            f"; subgroup for '{group}' must be "
        )

        if valid_values:
            valid_values_with_quotes = [f"'{vv}'" for vv in valid_values]
            message += f"in {', '.join(valid_values_with_quotes)}"

        else:
            message += f"{None}"

        super().__init__(message)
