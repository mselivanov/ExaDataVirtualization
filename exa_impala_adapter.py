"""
Module implements callback functions for Impala adapter.
"""
import json
from exa_impala_adapter_meta import AdapterRequestFactory


def adapter_call(request):
    request_dict = json.loads(request)
    adapter_request = AdapterRequestFactory.create_from_json_dict(request_dict)
    raise NotImplementedError
