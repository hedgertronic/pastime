import io
from datetime import date, timedelta
from typing import cast

from pastime.exceptions import InvalidBoundError, InvalidSubgroupError
from pastime.field import STATCAST_FIELDS, Collection, DateField, Field, MetricField
from pastime.query import Query
from pastime.type_aliases import Param


#######################################################################################
# CONSTANTS FOR REQUEST SPLITTING


MAX_ROWS_PER_REQUEST = 15_000


GAMES_PER_REGULAR_SEASON = 2_430
GAMES_PER_POSTSEASON = 53
GAMES_PER_YEAR = GAMES_PER_REGULAR_SEASON + GAMES_PER_POSTSEASON


PITCHES_PER_GAME = 325
PITCHES_PER_YEAR = GAMES_PER_YEAR * PITCHES_PER_GAME


#######################################################################################
# OTHER USEFUL INFO


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
# QUERY CLASSES


class SearchQuery(Query):
    ####################################################################################
    # PUBLIC METHODS

    def __init__(
        self,
        url: str,
        collection_name: str = "search",
        fields: dict[str, Collection] = STATCAST_FIELDS,
        **kwargs: Param,
    ):
        self.frequency = 1.0
        self.metric_counter = 1
        self.requests_to_make: list[dict[str, list[str]]] = []

        super().__init__(url, collection_name, fields, **kwargs)

        self._update_dates()

    def update_seasons(self) -> None:
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

    def request(self, **kwargs) -> io.StringIO:
        return super().request(messages=self._get_messages(), **kwargs)

    ####################################################################################
    # HELPER METHODS

    def _add_param(self, field: Field, values: Param) -> None:
        if not values:
            return

        new_params = field.get_params(values)

        if isinstance(field, DateField):
            pass

        elif isinstance(field, MetricField):
            new_params[f"metric_{self.metric_counter}"] = new_params.pop("metric")
            new_params[f"metric_{self.metric_counter}_gt"] = new_params.pop("metric_gt")
            new_params[f"metric_{self.metric_counter}_lt"] = new_params.pop("metric_lt")

            self.metric_counter += 1

        self.params |= new_params
        self.frequency *= field.get_frequency(self.params)

    def _update_dates(self) -> None:
        start_field = self.collection.fields["start_date"]
        end_field = self.collection.fields["end_date"]

        seasons = cast(
            list[str], self.collection.fields["year"].get_values(self.params)
        )

        start = (
            cast(date | None, start_field.get_values(self.params))
            or SEASON_DATES[int(seasons[0])]["start"]
        )
        end = (
            cast(date | None, end_field.get_values(self.params))
            or SEASON_DATES[int(seasons[-1])]["end"]
        )

        if start > end:
            raise InvalidBoundError(min_value=str(start), max_value=str(end))

        self._add_param(start_field, start)
        self._add_param(end_field, end)

    def _prepare_requests(self):
        request_date_pairs = self._get_date_pairs()

        for start_date, end_date in request_date_pairs:
            params_copy = self.params.copy()

            params_copy["game_date_gt"] = [start_date] if start_date else [""]
            params_copy["game_date_lt"] = [end_date] if end_date else [""]

            self.requests_to_make.append(params_copy)

    def _get_date_pairs(self) -> list[tuple[str, str]]:
        date_pairs: list[tuple[str, str]] = []

        start = cast(date, self.collection.fields["start_date"].get_values(self.params))
        end = cast(date, self.collection.fields["end_date"].get_values(self.params))

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
                " are not available before 2015."
            )

        if end and end >= date.today():
            messages.append(
                "Data is updated every day at 3 am."
                " Some of today's data may be missing."
            )

        return messages


class LeaderboardQuery(Query):
    def __init__(
        self,
        url: str,
        collection_name: str = "search",
        fields: dict[str, Collection] = STATCAST_FIELDS,
        **kwargs: Param,
    ):
        super().__init__(url, collection_name, fields, **kwargs)

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
