import io

from pastime.download import download_files
from pastime.exceptions import FieldNameError
from pastime.field import Collection, Field
from pastime.type_aliases import Param


class Query:
    ####################################################################################
    # PUBLIC METHODS

    def __init__(
        self,
        url: str,
        collection_name: str,
        fields: dict[str, Collection],
        **kwargs: Param,
    ):
        self.url = url

        collection = fields.get(collection_name)

        if not collection:
            raise FieldNameError(
                field_name=collection_name,
                valid_values=fields.keys(),
            )

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
        self._prepare_requests()

        return download_files(
            url=f"{self.url}/{self.collection.slug}",
            params=self.requests_to_make,
            **kwargs,
        )

    ####################################################################################
    # HELPER METHODS

    def _add_param(self, field: Field, values: Param) -> None:
        self.params |= field.get_params(values)

    def _prepare_requests(self):
        self.requests_to_make.append(self.params)
