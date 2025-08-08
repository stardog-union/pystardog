from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Awaitable, List, Optional, cast

if TYPE_CHECKING:
    from .client import BaseClient, ResponseType

import httpx
from pydantic import BaseModel, Field, computed_field, ConfigDict


class VoiceboxAppSettings(BaseModel):
    """Voicebox App settings. Some settings can be edited in the Stardog Cloud Portal."""

    name: str
    """name of the Voicebox App"""
    database: str
    """Stardog database the Voicebox App was configured with"""
    model: str
    """The model within the Stardog database"""
    named_graphs: List[str]
    """The named graphs within the Stardog database Voicebox will execute queries against"""
    reasoning: bool
    """Whether Voicebox should use reasoning or not in its queries to the Stardog database."""


class VoiceboxAction(BaseModel):
    model_config = ConfigDict(extra="allow")

    label: Optional[str] = None
    type: str
    value: str


class VoiceboxAnswer(BaseModel):
    model_config = ConfigDict(extra="allow")

    content: str
    """The main response to your question. This is intended to display to an end user."""
    conversation_id: str
    """The id (UUID) of the Voicebox conversation. Can be provided to different methods
    on the :class:`stardog.cloud.voicebox.VoiceboxApp` class to continue an existing conversation.
    """
    message_id: str
    """The id (UUID) of the Voicebox message."""
    actions: List[VoiceboxAction] = Field(default_factory=list)
    """The raw "actions" returned by Voicebox. Generally, you can just use the properties like :obj:`stardog.cloud.voicebox.VoiceboxAnswer.interpreted_question`
    or :obj:`stardog.cloud.voicebox.VoiceboxAnswer.sparql_query` instead of filtering the actions for these specific ones."""

    @computed_field  # type: ignore[misc]
    @property
    def interpreted_question(self) -> Optional[str]:
        """How Voicebox interpreted your question."""
        return next(
            (
                action.value
                for action in self.actions
                if action.type == "rewritten_query"
            ),
            None,
        )

    @computed_field  # type: ignore[misc]
    @property
    def sparql_query(self) -> Optional[str]:
        """The SPARQL query used to generate the response."""
        return next(
            (action.value for action in self.actions if action.type == "sparql"),
            None,
        )


class VoiceboxApp:
    """The Voicebox App created in the Stardog Cloud Portal."""

    def __init__(
        self,
        client: BaseClient,
        app_api_token: str,
        client_id: Optional[str] = None,
    ):
        """
        :param client: the Stardog Cloud API Client
        :param app_api_token: the Voicebox app API token from Stardog Cloud
        :param client_id: a unique identifier to specify the identity of the Voicebox user
        """
        self.app_api_token = app_api_token
        self.client = client
        self.client_id = client_id

    def _create_headers(
        self,
        api_token: str,
        client_id: Optional[str] = None,
        stardog_auth_token_override: Optional[str] = None,
    ) -> dict:
        """Create HTTP headers needed for Voicebox requests"""
        headers = {
            "Authorization": f"Bearer {api_token}",
            "X-Client-Id": client_id or self.client_id,
        }
        if stardog_auth_token_override:
            headers["X-SD-Auth-Token"] = stardog_auth_token_override
        return headers

    def _check_client_id(self, client_id: Optional[str]):
        if not self.client_id and not client_id:
            raise ValueError("client_id required")

    def _validate_conversation_id(self, conversation_id: Optional[str]):
        """Validate that conversation_id is a valid UUID format if provided"""
        if conversation_id is not None:
            try:
                uuid.UUID(conversation_id)
            except ValueError:
                raise ValueError(
                    f"conversation_id must be a valid UUID format, got: {conversation_id}"
                )

    async def _ensure_response(self, response: ResponseType) -> httpx.Response:
        """Helper method to handle both sync and async responses"""
        if isinstance(response, Awaitable):
            return await response
        return response

    def settings(self) -> VoiceboxAppSettings:
        """
        Returns the Voicebox application information registered with the ``app_api_token``
        """
        # API requires a client id in header but doesn't actually do anything
        headers = self._create_headers(
            self.app_api_token, self.client_id or "pystardog"
        )

        response = cast(
            httpx.Response,
            self.client._get(
                path="/v1/app",
                headers=headers,
            ),
        )
        data = response.json()
        return VoiceboxAppSettings(
            name=data.get("name", ""),
            database=data.get("database", ""),
            model=data.get("model", ""),
            named_graphs=data.get("named_graphs", []),
            reasoning=data.get("reasoning", False),
        )

    async def async_settings(self) -> VoiceboxAppSettings:
        """
        .. note::
            Async version of :obj:`stardog.cloud.voicebox.VoiceboxApp.settings`

        Returns the Voicebox application information registered with the ``app_api_token``

        """

        # API requires a client id in header but doesn't actually do anything
        headers = self._create_headers(
            self.app_api_token, self.client_id or "pystardog"
        )

        response = await self._ensure_response(
            self.client._get(
                path="/v1/app",
                headers=headers,
            )
        )
        data = response.json()
        return VoiceboxAppSettings(
            name=data.get("name", ""),
            database=data.get("database", ""),
            model=data.get("model", ""),
            named_graphs=data.get("named_graphs", []),
            reasoning=data.get("reasoning", False),
        )

    def ask(
        self,
        question: str,
        conversation_id: Optional[str] = None,
        client_id: Optional[str] = None,
        stardog_auth_token_override: Optional[str] = None,
    ) -> VoiceboxAnswer:
        """
        Ask a question to Voicebox.

        :param question: the question to ask Voicebox, e.g. ``"How many products were sold in 2024?"``
        :param conversation_id: the id of the Voicebox conversation on Stardog Cloud. If not provided, a new conversation will be created and the conversation id will be returned in the response.
        :param client_id: only required only if ``client_id`` was not provided when creating
            a ``Voicebox`` instance
        :param stardog_auth_token_override: optional bearer token to override the default Stardog token associated with your Voicebox app token. This is especially useful when your Voicebox App connects to Stardog via an SSO provider (e.g., Microsoft Entra) and you need to supply your own SSO-issued token to authenticate requests to your Stardog server
        """
        self._check_client_id(client_id)
        self._validate_conversation_id(conversation_id)

        headers = self._create_headers(
            self.app_api_token,
            client_id or self.client_id,
            stardog_auth_token_override,
        )

        response = cast(
            httpx.Response,
            self.client._post(
                path="/v1/voicebox/ask",
                json={"query": question, "conversation_id": conversation_id},
                headers=headers,
            ),
        )

        data = response.json()

        return VoiceboxAnswer(
            content=data.get("result"),
            message_id=data.get("message_id"),
            conversation_id=data.get("conversation_id"),
            actions=data.get("actions", []),
        )

    async def async_ask(
        self,
        question: str,
        conversation_id: Optional[str] = None,
        client_id: Optional[str] = None,
        stardog_auth_token_override: Optional[str] = None,
    ):
        """
        Ask Voicebox to generate a SPARQL query based on a natural language question.

        .. note::
            Async version of :obj:`stardog.cloud.voicebox.VoiceboxApp.ask`

        :param question: the question to ask Voicebox, e.g. ``"How many products were sold in 2024?"``
        :param conversation_id: the id of the Voicebox conversation on Stardog Cloud. If not provided, a new conversation will be created and the conversation id will be returned in the response.
        :param client_id: only required if ``client_id`` was not provided when creating
            a :class:`stardog.cloud.voicebox.VoiceboxApp` instance
        :param stardog_auth_token_override: optional bearer token to override the default Stardog token associated with your Voicebox app token. This is especially useful when your Voicebox App connects to Stardog via an SSO provider (e.g., Microsoft Entra) and you need to supply your own SSO-issued token to authenticate requests to your Stardog server


        """
        self._check_client_id(client_id)
        self._validate_conversation_id(conversation_id)

        headers = self._create_headers(
            self.app_api_token,
            client_id or self.client_id,
            stardog_auth_token_override,
        )

        response = await self._ensure_response(
            self.client._post(
                path="/v1/voicebox/ask",
                json={"query": question, "conversation_id": conversation_id},
                headers=headers,
            )
        )

        data = response.json()
        return VoiceboxAnswer(
            content=data.get("result"),
            message_id=data.get("message_id"),
            conversation_id=data.get("conversation_id"),
            actions=data.get("actions", []),
        )

    def generate_query(
        self,
        question: str,
        conversation_id: Optional[str] = None,
        client_id: Optional[str] = None,
        stardog_auth_token_override: Optional[str] = None,
    ):
        """
        Ask Voicebox to generate a SPARQL query based on a natural language question.

        :param question: the question to ask Voicebox, e.g. ``"How many products were sold in 2024?"``
        :param conversation_id: the id of the Voicebox conversation on Stardog Cloud. If not provided, a new conversation will be created and the conversation id will be returned in the response.
        :param client_id: only required if ``client_id`` was not provided when creating
            a :class:`stardog.cloud.voicebox.VoiceboxApp` instance
        :param stardog_auth_token_override: optional bearer token to override the default Stardog token associated with your Voicebox app token. This is especially useful when your Voicebox App connects to Stardog via an SSO provider (e.g., Microsoft Entra) and you need to supply your own SSO-issued token to authenticate requests to your Stardog server
        """
        self._check_client_id(client_id)
        self._validate_conversation_id(conversation_id)

        headers = self._create_headers(
            self.app_api_token,
            client_id or self.client_id,
            stardog_auth_token_override,
        )

        response = cast(
            httpx.Response,
            self.client._post(
                path="/v1/voicebox/generate-query",
                json={"query": question, "conversation_id": conversation_id},
                headers=headers,
            ),
        )

        data = response.json()
        return VoiceboxAnswer(
            content=data.get("result"),
            message_id=data.get("message_id"),
            conversation_id=data.get("conversation_id"),
            actions=data.get("actions", []),
        )

    async def async_generate_query(
        self,
        question: str,
        conversation_id: Optional[str] = None,
        client_id: Optional[str] = None,
        stardog_auth_token_override: Optional[str] = None,
    ):
        """
        Ask Voicebox to generate a SPARQL query based on a natural language question.

        .. note::
            Async version of :obj:`stardog.cloud.voicebox.VoiceboxApp.generate_query`

        :param question: the question to ask Voicebox, e.g. ``"How many products were sold in 2024?"``
        :param conversation_id: the id of the Voicebox conversation on Stardog Cloud. If not provided, a new conversation will be created and the conversation id will be returned in the response.
        :param client_id: only required if ``client_id`` was not provided when creating
            a :class:`stardog.cloud.voicebox.VoiceboxApp` instance
        :param stardog_auth_token_override: optional bearer token to override the default Stardog token associated with your Voicebox app token. This is especially useful when your Voicebox App connects to Stardog via an SSO provider (e.g., Microsoft Entra) and you need to supply your own SSO-issued token to authenticate requests to your Stardog server
        """
        self._check_client_id(client_id)
        self._validate_conversation_id(conversation_id)

        headers = self._create_headers(
            self.app_api_token,
            client_id or self.client_id,
            stardog_auth_token_override,
        )

        response = await self._ensure_response(
            self.client._post(
                path="/v1/voicebox/generate-query",
                json={"query": question, "conversation_id": conversation_id},
                headers=headers,
            )
        )

        data = response.json()
        return VoiceboxAnswer(
            content=data.get("result"),
            message_id=data.get("message_id"),
            conversation_id=data.get("conversation_id"),
            actions=data.get("actions", []),
        )
