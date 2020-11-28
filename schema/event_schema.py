from typing import Any
from typing import Dict
from typing import Set

from schema import attributes


REQUIRED_EVENT_ATTRS_TO_TYPES: Dict[str, Set[Any]] = {
    attributes.EVENT_NAME_FIELD: {str},
    attributes.LAT_FIELD: {int, float},
    attributes.LNG_FIELD: {int, float},
    attributes.OCCURRED_AT_MS_FIELD: {int, float, str},
}


def is_valid_event(event: Dict[Any, Any]) -> bool:
    for event_attr_name, expected_type in REQUIRED_EVENT_ATTRS_TO_TYPES.items():
        if event_attr_name not in event:
            return False

        if type(event[event_attr_name]) not in expected_type:
            return False

    return True
