from collections import namedtuple

StatefulAggregationResult = namedtuple(
    "StatefulAggregationResult", ["key", "feature_name", "value"],
)
