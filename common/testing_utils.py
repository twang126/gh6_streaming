from typing import Any
from typing import List
import apache_beam as beam


class _WriteToMemorySink(beam.DoFn):
    results: List[Any] = []

    def process(self, element):  # type: ignore
        _WriteToMemorySink.results.append(element)


class MemorySink(beam.PTransform):
    def __init__(self):
        self.write_do_fn = _WriteToMemorySink()

    def get_results(self):
        return self.write_do_fn.results

    def expand(self, pcol):
        pcol | beam.ParDo(self.write_do_fn)
