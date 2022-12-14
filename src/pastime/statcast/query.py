"""Organize and make a query to a collection of fields."""

from datetime import date, timedelta
from typing import cast

from pastime.exceptions import InvalidBoundError, InvalidSubgroupError
from pastime.field import Collection, DateField, Field, MetricField, Param
from pastime.query import Query


#######################################################################################
# CONSTANTS FOR REQUEST SPLITTING


# The number of games on a usual day
GAMES_PER_DAY = 15

# The number of games in a regular season
GAMES_PER_REGULAR_SEASON = 2_430

# The maximum number of games in the modern postseason
GAMES_PER_POSTSEASON = 53

# The maximum number of games in a year, including the regular season and postseason
GAMES_PER_YEAR = GAMES_PER_REGULAR_SEASON + GAMES_PER_POSTSEASON

# The average number of pitches in a game (both teams combined)
PITCHES_PER_GAME = 325

# The estimated number of pitches in a season
PITCHES_PER_YEAR = GAMES_PER_YEAR * PITCHES_PER_GAME

# The maximum number of rows in a request; despite a request returning up to 40,000
# rows, performance gets significantly slower the higher this number is
MAX_ROWS_PER_REQUEST = 15_000

# The estimated number of days to include in a request so that a request does not
# exceed the maximum number of rows in a request
DAYS_PER_REQUEST = int(MAX_ROWS_PER_REQUEST / (PITCHES_PER_GAME * GAMES_PER_DAY))


#######################################################################################
# OTHER USEFUL CONSTANTS


# The start date of every regular season and end date of every postseason
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


# Subgroups which are allowed for each group of the Swing-Take leaderboard
_SWING_TAKE_GROUPS = {
    "Swing-Take": ["swing", "take"],
    "Pitch Type": ["fastball", "curve", "cutter", "changeup", "slider", "other"],
    "Attack Region": ["heart", "shadow", "chase", "waste"],
    "Bat-side": ["R", "L"],
}


#######################################################################################
# QUERY CLASSES


class SearchQuery(Query):
    """Query a Statcast Search collection with given parameters.

    Attributes:
        url (str): The URL of the query.
        collection (Collection): The collection to query.
        params (dict[str, list[str]]): The params to URL-encoded and included in the
            request.
        requests_to_make (list[dict[str, list[str]]]): A list of param dicts that can
            be broken up by date if necessary. For collections that limit the number of
            items returned in a single request, multiple requests can be made to access
            all of the desired data. Statcast requests are limited to 40,000 rows.
        frequency (float): The estimated frequency factor of the request. This can be
            used to break up requests if necessary.
        metric_counter (int): The number of metrics included in the request. This is
            used since metrics included in a Statcast request must have an incrementing
            counter included in their slug.
    """

    ####################################################################################
    # PUBLIC METHODS

    def __init__(
        self,
        url: str,
        collection: Collection,
        **kwargs: Param,
    ):
        """Initialize a query with a collection of parameters.

        Args:
            url (str): The URL of the query.
            collection (Collection): The collection to query.
            kwargs (Param): The params to include in the query.

        Raises:
            FieldNameError: If the given field name does not exist for the collection.
        """
        self.frequency = 1.0
        self.metric_counter = 1

        super().__init__(url=url, collection=collection, **kwargs)

        self._update_dates()

    def update_years(self) -> None:
        """Update the years in a request to match the given dates.

        Since the default year in a request is the current year and the current year
        only, a request that is made with specified dates that are outside the current
        year will not include some of the expected data if the year value is not
        updated as well. Calling this method will automatically update the year param
        to contain all years between the given dates.

        For example, if a request is made for the dates `2019-05-01` through
        `2020-06-01` but the year parameter is left untouched, an empty DataFrame will
        be returned. This method will automatically update the year parameter to
        include 2019 and 2020.
        """
        season_field = self.collection.fields["year"]

        seasons = cast(list[str], season_field.get_values(self.params))

        start = cast(
            date | None, self.collection.fields["start_date"].get_values(self.params)
        )
        end = cast(
            date | None, self.collection.fields["end_date"].get_values(self.params)
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

    ####################################################################################
    # HELPER METHODS

    def _add_param(self, field: Field, values: Param) -> None:
        """Add a param to the query and update the frequency of the request."""
        if not values:
            return

        new_params = field.get_params(values)

        if isinstance(field, DateField):
            pass

        # Metrics included in a Statcast Search request contain an incrementing counter
        # in their URL slug for each additional request included.

        elif isinstance(field, MetricField):
            new_params[f"metric_{self.metric_counter}"] = new_params.pop("metric")
            new_params[f"metric_{self.metric_counter}_gt"] = new_params.pop("metric_gt")
            new_params[f"metric_{self.metric_counter}_lt"] = new_params.pop("metric_lt")

            self.metric_counter += 1

        self.params |= new_params
        self.frequency *= field.get_frequency(self.params)

    def _update_dates(self) -> None:
        """Update the dates included in the request.

        If no start or end date is included, the start or end date of the request will
        be updated to be the start or end date of the first or last season in the
        request.
        """
        start_field = cast(DateField, self.collection.fields["start_date"])
        end_field = cast(DateField, self.collection.fields["end_date"])

        seasons = cast(
            list[str], self.collection.fields["year"].get_values(self.params)
        )

        start = (
            start_field.get_values(self.params)
            or SEASON_DATES[int(seasons[0])]["start"]
        )
        end = end_field.get_values(self.params) or SEASON_DATES[int(seasons[-1])]["end"]

        if start > end:
            raise InvalidBoundError(min_value=str(start), max_value=str(end))

        self._add_param(start_field, start)
        self._add_param(end_field, end)

    def _prepare_requests(self) -> None:
        """Prepare the list of requests to make.

        The requests will be separated by start and end date if the frequency of the
        request is high enough to expect that there will be more than the maximum
        returned.
        """
        request_date_pairs = self._get_date_pairs()

        for start_date, end_date in request_date_pairs:
            params_copy = self.params.copy()

            params_copy["game_date_gt"] = [start_date] if start_date else [""]
            params_copy["game_date_lt"] = [end_date] if end_date else [""]

            self.requests_to_make.append(params_copy)

    def _get_date_pairs(self) -> list[tuple[str, str]]:
        """Separate the dates of the query based on expected rows returned."""
        date_pairs: list[tuple[str, str]] = []

        start = cast(date, self.collection.fields["start_date"].get_values(self.params))
        end = cast(date, self.collection.fields["end_date"].get_values(self.params))

        est_rows = PITCHES_PER_YEAR * self.frequency

        if est_rows < MAX_ROWS_PER_REQUEST:
            return [(str(start), str(end))]

        # Each request is limited to one season.

        for season in range(start.year, end.year + 1):
            season_start = SEASON_DATES[season]["start"]
            season_end = SEASON_DATES[season]["end"]

            range_start = max(season_start, start)

            while range_start <= min(season_end, end):
                range_end = min(
                    end,
                    range_start
                    + timedelta(days=int((DAYS_PER_REQUEST - 1) / self.frequency)),
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
        """Get any important messages for the given request."""
        messages = []

        start = cast(
            date | None, self.collection.fields["start_date"].get_values(self.params)
        )
        end = cast(
            date | None, self.collection.fields["end_date"].get_values(self.params)
        )

        if start and start < date(2008, 1, 1):
            messages.append(
                "Statcast data is only available from the 2008 season onwards."
            )

        if start and start < date(2015, 1, 1):
            messages.append(
                "Some metrics such as 'exit velocity' and 'batted ball events'"
                " are not available before 2015. The ability to add spin columns"
                " for data before 2015 is not possible."
            )

        if end and end >= date.today():
            messages.append(
                "Data is updated every day at 3 am."
                " Some of today's data may be missing."
            )

        return messages


class LeaderboardQuery(Query):
    """Query a Statcast Leaderboard collection with given parameters.

    Attributes:
        url (str): The URL of the query.
        collection (Collection): The collection to query.
        params (dict[str, list[str]]): The params to URL-encoded and included in the
            request.
        requests_to_make (list[dict[str, list[str]]]): A list of param dicts that can
            be broken up by date if necessary. For collections that limit the number of
            items returned in a single request, multiple requests can be made to access
            all of the desired data. Statcast requests are limited to 40,000 rows.
        frequency (float): The estimated frequency factor of the request. This can be
            used to break up requests if necessary.
        metric_counter (int): The number of metrics included in the request. This is
            used since metrics included in a Statcast request must have an incrementing
            counter included in their slug.
    """

    def __init__(
        self,
        url: str,
        collection: Collection,
        **kwargs: Param,
    ):
        """Initialize a query with a collection of parameters.

        If the collection is the Swing-Take leaderboard, an additional check must be
        made to ensure the chosen subgroup is compatible with the chosen group.

        Args:
            url (str): The URL of the query.
            collection (Collection): The collection to query.
            kwargs (Param): The params to include in the query.

        Raises:
            FieldNameError: If the given field name does not exist for the collection.
        """
        super().__init__(url=url, collection=collection, **kwargs)

        if self.collection.name == "swing_take":
            group = self.params["type"][0]
            subgroup = self.params.get("sub_type", [""])[0]

            if subgroup and subgroup.lower() not in _SWING_TAKE_GROUPS.get(group, [""]):
                raise InvalidSubgroupError(
                    group=group,
                    subgroup=subgroup,
                    leaderboard=self.collection.name,
                    valid_values=_SWING_TAKE_GROUPS.get(group, None),
                )

        self.params["csv"] = ["true"]
