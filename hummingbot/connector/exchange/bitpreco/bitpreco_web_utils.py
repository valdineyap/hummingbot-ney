import time
from typing import Any, Dict, Optional

import hummingbot.connector.exchange.bitpreco.bitpreco_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    if path_url.startswith("http"):
        return path_url
    return CONSTANTS.REST_URL


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    if path_url.startswith("http"):
        return path_url
    return CONSTANTS.REST_URL


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        auth: Optional[AuthBase] = None, ) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth)
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def api_request(path: str,
                      api_factory: Optional[WebAssistantsFactory] = None,
                      throttler: Optional[AsyncThrottler] = None,
                      params: Optional[Dict[str, Any]] = None,
                      data: Optional[Dict[str, Any]] = None,
                      method: RESTMethod = RESTMethod.GET,
                      is_auth_required: bool = False,
                      return_err: bool = False,
                      limit_id: Optional[str] = None,
                      timeout: Optional[float] = None,
                      headers: Dict[str, Any] = {}):
    throttler = throttler or create_throttler()

    # If api_factory is not provided a default one is created
    # The default instance has no authentication capabilities and all authenticated requests will fail
    api_factory = api_factory or build_api_factory(
        throttler=throttler,
    )
    rest_assistant = await api_factory.get_rest_assistant()

    local_headers = {
        "Content-Type": "application/json"}
    local_headers.update(headers)
    url = public_rest_url(path_url=path, domain="")

    request = RESTRequest(
        method=method,
        url=url,
        params=params,
        data=data,
        headers=local_headers,
        is_auth_required=is_auth_required,
        throttler_limit_id=limit_id if limit_id else path
    )

    async with throttler.execute_task(limit_id=limit_id if limit_id else path):
        response = await rest_assistant.call(request=request, timeout=timeout)
        if response.status != 200:
            if return_err:
                error_response = await response.json()
                return error_response
            else:
                error_response = await response.text()
                if error_response is not None and "ret_code" in error_response and "ret_msg" in error_response:
                    raise IOError(f"The request to BitPreco failed. Error: {error_response}. Request: {request}")
                else:
                    raise IOError(f"Error executing request {method.name} {path}. "
                                  f"HTTP status is {response.status}. "
                                  f"Error: {error_response}")

        return await response.json()


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    # TODO acquire server time by requesting API
    t = time.time()
    server_time_ms = int(t * 1000)
    return server_time_ms
