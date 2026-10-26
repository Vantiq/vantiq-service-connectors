"""Tests for the Vantiq type support surfaced by the service SDK: the re-exported types, JSON encoding of
their values in a connector response, and the @procedure decorator."""
import datetime as dt
import json
from decimal import Decimal

import pytest

from vantiqservicesdk import (
    BaseVantiqServiceConnector,
    Currency,
    GeoJSON,
    InvocationStyle,
    ResourceReference,
    VantiqType,
    from_vantiq,
    procedure,
    to_vantiq,
)


def test_types_are_re_exported_from_vantiqsdk():
    import vantiqsdk
    assert Currency is vantiqsdk.Currency
    assert GeoJSON is vantiqsdk.GeoJSON
    assert ResourceReference is vantiqsdk.ResourceReference
    assert VantiqType is vantiqsdk.VantiqType
    assert from_vantiq is vantiqsdk.from_vantiq and to_vantiq is vantiqsdk.to_vantiq


class _Connector(BaseVantiqServiceConnector):
    # Skips the base constructor: it registers Prometheus metrics, which can only happen once per process.
    def __init__(self):
        pass


def test_response_encodes_vantiq_values():
    connector = _Connector()
    response = {'result': {'total': Currency('12.34', 'USD'), 'ratio': Decimal('0.5'),
                           'when': dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)}}
    encoded = json.loads(connector.encode_response(response))
    assert encoded == {'result': {'total': 'USD:12.34', 'ratio': '0.5', 'when': '2024-01-02T03:04:05Z'}}


def test_response_still_rejects_unknown_objects():
    with pytest.raises(TypeError):
        _Connector().encode_response({'result': object()})


def test_to_json_override_adds_types():
    class Connector(_Connector):
        def to_json(self, obj):
            if isinstance(obj, set):
                return sorted(obj)
            return super().to_json(obj)

    encoded = json.loads(Connector().encode_response({'result': [{2, 1}, Decimal('1')]}))
    assert encoded == {'result': [[1, 2], '1']}


def test_procedure_records_overrides_and_returns_function():
    @procedure(return_type='Currency', param_types={'amount': 'Currency'}, invocation_style='streaming')
    def total(amount):
        return amount

    assert total.__vantiq_procedure__ == {'return_type': 'Currency', 'param_types': {'amount': 'Currency'},
                                          'invocation_style': 'streaming'}
    assert total(3) == 3


def test_procedure_accepts_enum_and_bare_use():
    @procedure(invocation_style=InvocationStyle.streaming)
    def one():
        pass

    @procedure
    def two():
        pass

    assert one.__vantiq_procedure__ == {'invocation_style': 'streaming'}
    assert two.__vantiq_procedure__ == {}


def test_procedure_validates_arguments():
    with pytest.raises(TypeError):
        procedure(return_type=5)
    with pytest.raises(TypeError):
        procedure(param_types={'a': 1})
    with pytest.raises(ValueError):
        procedure(invocation_style='batch')
    with pytest.raises(TypeError):
        procedure(invocation_style=1)
