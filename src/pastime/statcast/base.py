import json
from typing import Any

import pkg_resources

from pastime.field import Collection, construct_fields


_statcast_data: dict[str, dict[str, Any]] = json.load(
    pkg_resources.resource_stream(__name__, "data/statcast_fields.json")
)


# A mapping of collection names to their respective Statcast field collections
STATCAST_COLLECTIONS: dict[str, Collection] = {
    _collection_name: Collection(
        _field_data["name"],
        _field_data["slug"],
        construct_fields(_field_data["fields"]),
    )
    for _collection_name, _field_data in _statcast_data.items()
}
