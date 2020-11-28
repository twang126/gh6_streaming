import time

import apache_beam as beam
import pytest

from common import testing_utils
from schema import attributes
from transforms import default


@pytest.fixture
def sample_events():
    return [
        # gh5 is: v0000
        {
            attributes.EVENT_NAME_FIELD: "event_name",
            attributes.LAT_FIELD: 45,
            attributes.LNG_FIELD: 45,
            attributes.OCCURRED_AT_MS_FIELD: int(time.time() * 1000),
            "event_id": 1,
        },
        # missing event name field
        {
            attributes.LAT_FIELD: 45,
            attributes.LNG_FIELD: 45,
            attributes.OCCURRED_AT_MS_FIELD: int(time.time() * 1000),
        },
        # missing lat field
        {
            attributes.EVENT_NAME_FIELD: "event_name",
            attributes.LNG_FIELD: 45,
            attributes.OCCURRED_AT_MS_FIELD: int(time.time() * 1000),
        },
        # missing lng field
        {
            attributes.EVENT_NAME_FIELD: "event_name",
            attributes.LAT_FIELD: 45,
            attributes.OCCURRED_AT_MS_FIELD: int(time.time() * 1000),
        },
        # missing occured_at_ms field
        {
            attributes.EVENT_NAME_FIELD: "event_name",
            attributes.LAT_FIELD: 45,
            attributes.LNG_FIELD: 45,
        },
        # too out of date
        {
            attributes.EVENT_NAME_FIELD: "event_name",
            attributes.LAT_FIELD: 45,
            attributes.LNG_FIELD: 45,
            attributes.OCCURRED_AT_MS_FIELD: 0,
        },
    ]


def test_default_transform(sample_events):
    pipeline = beam.Pipeline()
    memory_sink = testing_utils.MemorySink()
    pipeline | beam.Create(sample_events,) | default.DefaultTransform() | memory_sink

    pipeline.run()
    results = memory_sink.get_results()

    assert len(results) == 1
    assert results[0]["event_id"] == 1
    assert results[0]["geohash"] == "v0000"
