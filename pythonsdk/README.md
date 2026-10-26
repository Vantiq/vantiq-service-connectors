## Repository Contents

This repository contains an SDK for building Vantiq Service Connectors in Python. Service connectors allow some of all
of a Vantiq service to be implemented outside the Vantiq platform.  The typical use case for this is to leverage a language other than VAIl (in this case Python).  One major use case for Python is to take advantage of its broad support for ML/Generative AI libraries.

## Overview of a Service Connector

TBD...   Python Service Connectors are only used internally by Vantiq at this point.

## Building a Service Connector

Running this code required Python 3.8 or later.

To use the SDK, install it into your Python environment.  Using `pip`, 

```commandline
pip install vantiqservicesdk
```

Other Python environment tools may use different commands.

Once it is installed, you can import it.

```python

from vantiqservicesdk import BaseVantiqServiceConnector
```

More details TBD...

## Vantiq types and procedure metadata

The SDK re-exports the Python forms of the Vantiq types that plain Python types cannot express, from
[vantiqsdk](https://github.com/Vantiq/vantiq-python-sdk/blob/master/docs/api.md#vantiq-types):
`Currency`, `GeoJSON`, `ResourceReference`, `VantiqType`, and the converters `from_vantiq` and
`to_vantiq`. A connector response is encoded with `vantiq_json_default`, so a procedure can return a
`Currency`, a `Decimal`, or a `datetime` directly; override `to_json` on the connector to encode further
types.

`@procedure` records overrides for a procedure of a Python service when its type hints cannot express
the Vantiq signature:

```python
from vantiqservicesdk import Currency, procedure

@procedure(return_type='Currency', param_types={'amount': 'Currency'})
def total_due(account: str, amount) -> object:
    ...
```

It accepts `return_type` (a Vantiq type name), `param_types` (parameter name to Vantiq type name), and
`invocation_style` (an `InvocationStyle` or its name), and stores them on the function as
`__vantiq_procedure__`.
