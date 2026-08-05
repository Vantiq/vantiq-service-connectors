"""Tests for the connector-shared utilities added to the SDK: InvocationStyle, VantiqClientContext,
and the Prometheus helpers (VantiqCollectorRegistry, CounterWithReset)."""
from vantiqservicesdk import (
    CounterWithReset,
    InvocationStyle,
    VantiqClientContext,
    VantiqCollectorRegistry,
)


def test_invocation_style_values_match_server_enum():
    assert InvocationStyle.standard.value == 0
    assert InvocationStyle.streaming.value == 1
    assert InvocationStyle.events.value == 2


def test_vantiq_client_context_is_subclassable():
    class Ctx(VantiqClientContext):
        def get_resource_cache(self):
            return "cache"

    assert Ctx().get_resource_cache() == "cache"


def test_counter_with_reset_zeroes_on_collect():
    registry = VantiqCollectorRegistry()
    counter = CounterWithReset('demo_total', 'demo', registry=registry)
    counter.inc(4)
    counter.collect()          # collect() returns the metrics and resets the counter
    assert counter._value.get() == 0


def test_registry_find_collector_by_name():
    registry = VantiqCollectorRegistry()
    CounterWithReset('lookup_total', 'demo', registry=registry)
    assert registry.find_collector_by_name('lookup_total') is not None
    assert registry.find_collector_by_name('nope') is None
