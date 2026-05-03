import json
from collections import OrderedDict
from typing import Any, Dict

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class BitprecoAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the auth_token into the data request, required for authenticated interactions.
        :param request: the request to be configured for authenticated interaction
        """
        if request.method == RESTMethod.POST:
            request.data = self._add_auth_token_to_data(data=json.loads(request.data))
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def _add_auth_token_to_data(self, data: Dict[str, Any]):
        request_params = OrderedDict(data or {})
        auth_token = f'{self.secret_key}{self.api_key}'
        request_params["auth_token"] = auth_token
        return request_params
