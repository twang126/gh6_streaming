from typing import Any
from typing import Dict
from schema import attributes


REQUIRED_EVENT_ATTRS_TO_TYPE: Dict[str, Any] = {
    attributes.EVENT_NAME_FIELD: str,
    attributes.LAT_FIELD: float,
    attributes.LNG_FIELD: float,
    attributes.OCCURRED_AT_MS_FIELD: float
}


def is_valid_event(event: Dict[Any, Any]) -> bool:
    for event_attr_name, expected_type in REQUIRED_EVENT_ATTRS_TO_TYPE.items():
        if event_attr_name not in event:
            return False

        
        if type(event[event_attr_name]) != expected_type:
            return False
        
    return True
