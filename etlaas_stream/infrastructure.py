import simplejson
from typing import Any


def default_loads(s: str) -> Any:
    return simplejson.loads(s, use_decimal=True)


def default_dumps(d: Any) -> str:
    return simplejson.dumps(d, use_decimal=True)
