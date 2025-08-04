import enum
import json
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Awaitable, Optional, Union

import httpx

from .exceptions import _API_STATUS_EXCEPTIONS, StardogCloudException
from .voicebox import VoiceboxApp


class StardogCloudAPIEndpoints(str, enum.Enum):
    """Stardog Cloud API endpoints"""

    US = "https://cloud.stardog.com/api"
    EU = "https://eu-cloud.stardog.com/api"


ResponseType = Union[httpx.Response, Awaitable[httpx.Response]]


class BaseClient(ABC):
    @property
    @abstractmethod
    def base_url(self) -> str:
        """The base URL of the Stardog Cloud API."""
        pass

    @abstractmethod
    def _post(self, path: str, **kwargs) -> ResponseType:
        pass

    @abstractmethod
    def _put(self, path: str, **kwargs) -> ResponseType:
        pass

    @abstractmethod
    def _get(self, path: str, **kwargs) -> ResponseType:
        pass

    @abstractmethod
    def _delete(self, path: str, **kwargs) -> ResponseType:
        pass

    @abstractmethod
    def voicebox_app(self, app_api_token: str, client_id: Optional[str] = None):
        """Initialize the Voicebox app."""
        pass

    def _handle_response(self, response: httpx.Response) -> httpx.Response:
        """
        Handle API responses and raise exceptions for HTTP errors.
        If an error is detected, it attempts to parse the response body for an error message
        and raises the appropriate exception.
        """

        if response.is_error:
            try:
                error_data = response.json()
            except json.JSONDecodeError:
                error_data = {"message": response.text}

            status_code = response.status_code
            msg = error_data.get("message", "") or error_data.get("detail")

            exception_class = _API_STATUS_EXCEPTIONS.get(
                status_code, StardogCloudException
            )
            raise exception_class(msg, status_code)

        return response


class Client(BaseClient):
    """
    A synchronous client for interacting with the Starodg Cloud API.

    .. code-block:: python
        :caption: Use ``with stardog.cloud.Client()`` if you want a context-managed client

        >>> with stardog.cloud.Client() as client:
        >>>     voicebox = client.voicebox_app(app_api_token="my-secret-token", client_id="some-id")
        >>>     answer = voicebox.ask(question="Who produced the most cars in 2023?")

    .. code-block:: python
        :caption: Alternatively, use ``client.close()`` if you want to close a client explicitly:
            **This is needed to avoid resource leaks.** There is no need to close ``close()`` if you are using a context-managed client.

        >>> client = stardog.cloud.Client()
        ...do stuff
        >>> client.close()

    """

    def __init__(
        self,
        base_url: str = StardogCloudAPIEndpoints.US.value,
        timeout: float = 30.0,
    ):
        """
        :param base_url: The base URL of the Stardog Cloud API.
        :param timeout: Request timeout in seconds.
        """
        self._base_url = base_url
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    @property
    def base_url(self) -> str:
        """The base URL of the Stardog Cloud API."""
        return self._base_url

    def __enter__(self):
        self._client.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ):
        self._client.__exit__(exc_type, exc_val, exc_tb)

    def close(self):
        """Close transport and proxies. Should be called if this client instance is not
        being used as a context manager."""
        self._client.close()

    def _post(self, path: str, **kwargs) -> httpx.Response:
        response = self._client.post(path, **kwargs)
        return self._handle_response(response)

    def _put(self, path: str, **kwargs) -> httpx.Response:
        response = self._client.put(path, **kwargs)
        return self._handle_response(response)

    def _get(self, path: str, **kwargs) -> httpx.Response:
        response = self._client.get(path, **kwargs)
        return self._handle_response(response)

    def _delete(self, path: str, **kwargs) -> httpx.Response:
        response = self._client.delete(path, **kwargs)
        return self._handle_response(response)

    def voicebox_app(
        self, app_api_token: str, client_id: str | None = None
    ) -> VoiceboxApp:
        """Initialize the Voicebox app."""
        return VoiceboxApp(
            client=self, app_api_token=app_api_token, client_id=client_id
        )


class AsyncClient(BaseClient):
    """
    An asynchronous client for interacting with the Starodg Cloud API.

    .. code-block:: python
        :caption: Use ``async with stardog.cloud.AsyncClient()`` if you want a context-managed client

        >>> async with stardog.cloud.AsyncClient() as client:
        >>>     voicebox = client.voicebox_app(app_api_token="my-secret-token", client_id="some-id")
        >>>     answer = await voicebox.async_ask(question="Who produced the most cars in 2023?")

    .. code-block:: python
        :caption: Alternatively, use ``await client.aclose()`` if you want to close a client explicitly.
            **This is needed to avoid resource leaks.** There is no need to close ``aclose()`` if you are using a context-managed client.

        >>> client = stardog.cloud.AsyncClient()
        ...do stuff
        >>> await client.aclose()

    """

    def __init__(
        self,
        base_url: str = StardogCloudAPIEndpoints.US.value,
        timeout: float = 30.0,
    ):
        """
        :param base_url: The base URL of the Stardog Cloud API.
        :param timeout: Request timeout in seconds.
        """
        self._base_url = base_url
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    @property
    def base_url(self) -> str:
        """The base URL of the Stardog Cloud API."""
        return self._base_url

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ):
        await self._client.__aexit__(exc_type, exc_val, exc_tb)

    async def aclose(self):
        """Close the transport and proxies. Should be called if this client instance is not
        being used as a context manager.
        """
        await self._client.aclose()

    async def _post(self, path: str, **kwargs) -> httpx.Response:
        response = await self._client.post(path, **kwargs)
        return self._handle_response(response)

    async def _put(self, path: str, **kwargs) -> httpx.Response:
        response = await self._client.put(path, **kwargs)
        return self._handle_response(response)

    async def _get(self, path: str, **kwargs) -> httpx.Response:
        response = await self._client.get(path, **kwargs)
        return self._handle_response(response)

    async def _delete(self, path: str, **kwargs) -> httpx.Response:
        response = await self._client.delete(path, **kwargs)
        return self._handle_response(response)

    def voicebox_app(
        self, app_api_token: str, client_id: str | None = None
    ) -> VoiceboxApp:
        """Initialize the Voicebox app."""
        return VoiceboxApp(
            client=self, app_api_token=app_api_token, client_id=client_id
        )
