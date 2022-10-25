"""Query a collection with given parameters."""

import io

from pastime.download import download_files
from pastime.exceptions import FieldNameError
from pastime.field import Collection, Field
from pastime.type_aliases import Param


class Query:
    """Query a collection with given parameters."""

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

        Raises:
            FieldNameError: If the given field name does not exist for the collection.
        """
        self.url = url
        self.collection = collection

        self.params: dict[str, list[str]] = {}
        self.requests_to_make: list[dict[str, list[str]]] = []

        for field_name, field_values in kwargs.items():
            field = self.collection.fields.get(field_name)

            if not field:
                raise FieldNameError(
                    field_name=field_name, valid_values=self.collection.fields.keys()
                )

            self._add_param(field, field_values)

    def request(self, **kwargs) -> io.StringIO:
        """Prepare and make all necessary requests.

        Returns:
            io.StringIO: All of the data returned from the request as a string IO
                object.
        """
        self._prepare_requests()

        return download_files(
            url=f"{self.url}/{self.collection.slug}",
            params=self.requests_to_make,
            **kwargs,
        )

    ####################################################################################
    # HELPER METHODS

    def _add_param(self, field: Field, values: Param) -> None:
        """Add a param to the query."""
        self.params |= field.get_params(values)

    def _prepare_requests(self) -> None:
        """Prepare the list of requests to make."""
        self.requests_to_make.append(self.params)
