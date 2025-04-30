"""
Benchmarks for LogPublisher event dispatching.
"""

import pytest
from twisted.logger import LogPublisher, LogLevel, LogEvent
from typing import List


class DummyObserver:
    """
    An observer that just records the last event it received.
    """
    def __init__(self):
        self.last_event = None

    def __call__(self, event: LogEvent):
        self.last_event = event


@pytest.mark.parametrize("num_observers", [100, 500, 1000])
def test_log_publisher_call_dispatch(benchmark, num_observers):
    """
    Benchmark the time it takes to dispatch an event to N observers.
    """

    observers: List[DummyObserver] = [DummyObserver() for _ in range(num_observers)]
    publisher = LogPublisher(*observers)

    event = {
        "log_level": LogLevel.info,
        "log_namespace": "benchmark",
        "log_format": "This is a benchmark event.",
    }

    def go():
        publisher(event)

    benchmark(go)


@pytest.mark.parametrize("num_observers", [100, 500, 1000])
def test_log_publisher_add_remove(benchmark, num_observers):
    """
    Benchmark the cost of adding and removing observers repeatedly.
    """

    publisher = LogPublisher()

    observers = [DummyObserver() for _ in range(num_observers)]

    def go():
        for obs in observers:
            publisher.addObserver(obs)
        for obs in observers:
            publisher.removeObserver(obs)

    benchmark(go)
