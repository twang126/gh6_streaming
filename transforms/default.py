from typing import Any
from typing import Dict

import apache_beam as beam

from common import geo_utils
from common import time_utils
from schema import attributes
from schema import event_schema


class _DefaultDoFn(beam.DoFn):
    def __init__(self, lateness_threshold_seconds: int = 60):
        self.lateness_threshold_seconds = 60

    def is_event_late(self, event: Dict[str, Any]) -> bool:
        lateness = abs(
            event[attributes.OCCURRED_AT_MS_FIELD] - time_utils.get_current_time_ms(),
        )

        return lateness > self.lateness_threshold_seconds * 1000

    def should_pass_event(self, event: Dict[str, Any]) -> bool:
        if not event_schema.is_valid_event(event,):
            return False

        if self.is_event_late(event):
            return False

        return True

    def annotate_fields_with_gh5(self, event: Dict[str, Any]) -> Dict[str, Any]:
        event[attributes.GEOHASH_FIELD] = geo_utils.lat_lng_to_geohash(
            event[attributes.LAT_FIELD], event[attributes.LNG_FIELD],
        )

        return event

    def process(self, event):
        if self.should_pass_event(event):
            yield self.annotate_fields_with_gh5(event)


class DefaultTransform(beam.PTransform):
    def __init__(self, lateness_threshold_seconds: int = 60):
        self.lateness_threshold_seconds = lateness_threshold_seconds

    def expand(self, pcol):  # type: ignore
        default_dofn = _DefaultDoFn(self.lateness_threshold_seconds)

        return pcol | beam.ParDo(default_dofn)
