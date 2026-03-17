import json
from contextlib import asynccontextmanager, contextmanager
from unittest.mock import MagicMock

import httpx
import pytest
import respx

from stardog.cloud.client import AsyncClient, Client
from stardog.cloud.voicebox import VoiceboxApp


def _make_ndjson_lines(events):
    """Helper: convert list of dicts to NDJSON line strings."""
    return [json.dumps(e) for e in events]


def _make_mock_stream_response(ndjson_dicts):
    """Create a mock response whose iter_lines() yields NDJSON strings."""
    lines = _make_ndjson_lines(ndjson_dicts)
    mock_response = MagicMock()
    mock_response.iter_lines.return_value = iter(lines)
    mock_response.is_error = False
    mock_response.status_code = 200
    return mock_response


async def _async_iter(items):
    """Helper async generator."""
    for item in items:
        yield item


def _make_async_mock_stream_response(ndjson_dicts):
    """Create a mock response whose aiter_lines() yields NDJSON strings asynchronously."""
    lines = _make_ndjson_lines(ndjson_dicts)
    mock_response = MagicMock()
    mock_response.aiter_lines.return_value = _async_iter(lines)
    mock_response.is_error = False
    mock_response.status_code = 200
    return mock_response


# Standard mode NDJSON events (multiple lines)
STANDARD_MODE_EVENTS = [
    {
        "result": "",
        "conversation_id": "550e8400-e29b-41d4-a716-446655440000",
        "message_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "pending": True,
        "actions": [
            {
                "type": "rewritten_query",
                "label": "Interpreted Question",
                "value": "How many products are in the database?",
            }
        ],
    },
    {
        "result": "",
        "conversation_id": "550e8400-e29b-41d4-a716-446655440000",
        "message_id": "7ca8c921-aebc-22e2-91c5-11d15fe541d9",
        "pending": True,
        "actions": [
            {
                "type": "rewritten_query",
                "label": "Interpreted Question",
                "value": "How many products are in the database?",
            },
            {
                "type": "sparql",
                "label": "SPARQL Query",
                "value": "PREFIX : <http://example.org/>\nSELECT (COUNT(?product) AS ?count)\nWHERE {\n  ?product a :Product .\n}",
            },
        ],
    },
    {
        "result": "Based on the data in your knowledge graph, there are 157 products currently in the database.",
        "conversation_id": "550e8400-e29b-41d4-a716-446655440000",
        "message_id": "8db9da32-bfcd-33f3-a2d6-22e26af652ea",
        "pending": False,
        "actions": [
            {
                "type": "rewritten_query",
                "label": "Interpreted Question",
                "value": "How many products are in the database?",
            },
            {
                "type": "sparql",
                "label": "SPARQL Query",
                "value": "PREFIX : <http://example.org/>\nSELECT (COUNT(?product) AS ?count)\nWHERE {\n  ?product a :Product .\n}",
            },
        ],
    },
]

# Single NDJSON event (used for simple stream tests)
SINGLE_NDJSON_EVENT = [
    {
        "result": "There are 157 products.",
        "conversation_id": "550e8400-e29b-41d4-a716-446655440000",
        "message_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "pending": False,
        "actions": [
            {
                "type": "sparql",
                "label": "SPARQL Query",
                "value": "SELECT (COUNT(?product) AS ?count) WHERE { ?product a :Product . }",
            },
        ],
    },
]


class TestVoiceboxAppSync:
    """Test VoiceboxApp with sync client and Stardog Cloud API responses"""

    def setup_method(self):
        """Set up test fixtures"""
        self.client = Client()
        self.voicebox = VoiceboxApp(
            client=self.client,
            app_api_token="test-app-token",
            client_id="test-client-id",
        )

    def _mock_stream_post(self, ndjson_dicts):
        """Return a contextmanager-patched _stream_post."""
        mock_response = _make_mock_stream_response(ndjson_dicts)

        @contextmanager
        def fake_stream_post(path, **kwargs):
            yield mock_response

        return fake_stream_post, mock_response

    @respx.mock
    def test_ask(self):
        """Test ask method"""
        api_response = {
            "result": "Based on the data in your knowledge graph, there are 157 products currently in the database. This includes products from various categories such as electronics, clothing, and books.",
            "conversation_id": "550e8400-e29b-41d4-a716-446655440000",
            "message_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "actions": [
                {
                    "type": "rewritten_query",
                    "label": "Interpreted Question",
                    "value": "How many products are in the database?",
                },
                {
                    "type": "sparql",
                    "label": "SPARQL Query",
                    "value": "PREFIX : <http://example.org/>\\nSELECT (COUNT(?product) AS ?count)\\nWHERE {\\n  ?product a :Product .\\n}",
                },
            ],
        }

        respx.post(f"{self.client.base_url}/v1/voicebox/ask").mock(
            return_value=httpx.Response(200, json=api_response)
        )

        result = self.voicebox.ask("How many products are there?")

        # Test end user model
        assert result.content == api_response["result"]
        assert result.conversation_id == api_response["conversation_id"]
        assert result.message_id == api_response["message_id"]
        assert result.interpreted_question == api_response["actions"][0]["value"]
        assert result.sparql_query == api_response["actions"][1]["value"]
        assert len(result.actions) == len(api_response["actions"])
        assert result.actions[1].type == api_response["actions"][1]["type"]

    @respx.mock
    def test_ask_with_auth_override(self):
        """Test ask with auth token override"""
        api_response = {
            "result": "Q4 revenue totaled $2.3M from enterprise customers.",
            "conversation_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "message_id": "f9e8d7c6-b5a4-3210-9876-543210fedcba",
            "actions": [
                {
                    "type": "sparql",
                    "value": 'SELECT (SUM(?revenue) AS ?total) WHERE { ?sale :revenue ?revenue ; :quarter "Q4" . }',
                },
            ],
        }

        mock_request = respx.post(f"{self.client.base_url}/v1/voicebox/ask").mock(
            return_value=httpx.Response(200, json=api_response)
        )

        result = self.voicebox.ask(
            "What were Q4 sales?", stardog_auth_token_override="sso-token-12345"
        )

        # Verify auth header
        assert (
            mock_request.calls.last.request.headers["X-SD-Auth-Token"]
            == "sso-token-12345"
        )

        # Test end user model
        assert result.content == api_response["result"]
        assert result.conversation_id == api_response["conversation_id"]
        assert result.message_id == api_response["message_id"]
        assert result.actions[0].type == api_response["actions"][0]["type"]
        assert result.actions[0].value == api_response["actions"][0]["value"]
        assert result.sparql_query == api_response["actions"][0]["value"]

    @respx.mock
    def test_generate_query(self):
        """Test generate_query method"""
        api_response = {
            "result": "PREFIX ex: <http://example.org/>\\nSELECT ?person ?name ?age\\nWHERE {\\n  ?person a ex:Employee ;\\n          ex:name ?name ;\\n          ex:age ?age .\\n  FILTER(?age > 30)\\n}",
            "conversation_id": "7f3d4c2e-8b1a-4f5e-9c6d-2a8b7e4f1c3d",
            "message_id": "9e2f8c4a-6b3d-4e1f-8a5c-7d9b2e4f6a8c",
            "actions": [
                {
                    "type": "rewritten_query",
                    "label": "Rewritten Query",
                    "value": "Find employees over 30 years old",
                },
                {
                    "type": "sparql",
                    "label": "Generated SPARQL",
                    "value": "PREFIX ex: <http://example.org/>\\nSELECT ?person ?name ?age\\nWHERE {\\n  ?person a ex:Employee ;\\n          ex:name ?name ;\\n          ex:age ?age .\\n  FILTER(?age > 30)\\n}",
                },
            ],
        }

        respx.post(f"{self.client.base_url}/v1/voicebox/generate-query").mock(
            return_value=httpx.Response(200, json=api_response)
        )

        result = self.voicebox.generate_query("Show me employees over 30")

        # Test end user model
        assert result.content == api_response["result"]
        assert result.conversation_id == api_response["conversation_id"]
        assert result.message_id == api_response["message_id"]
        assert result.interpreted_question == api_response["actions"][0]["value"]
        assert result.sparql_query == api_response["actions"][1]["value"]
        assert len(result.actions) == len(api_response["actions"])

    @respx.mock
    def test_settings(self):
        """Test settings method"""
        api_response = {
            "name": "Voicebox Integration",
            "database": "company-knowledge-graph",
            "model": "model_v1",
            "named_graphs": [
                "http://company.com/data",
            ],
            "reasoning": True,
        }

        respx.get(f"{self.client.base_url}/v1/app").mock(
            return_value=httpx.Response(200, json=api_response)
        )

        settings = self.voicebox.settings()

        # Test end user model
        assert settings.name == api_response["name"]
        assert settings.database == api_response["database"]
        assert settings.model == api_response["model"]
        assert settings.named_graphs == api_response["named_graphs"]
        assert settings.reasoning == api_response["reasoning"]

    def test_ask_with_invalid_conversation_id(self):
        """Test that invalid conversation_id raises ValueError"""
        with pytest.raises(
            ValueError,
            match="conversation_id must be a valid UUID format, got: invalid-uuid",
        ):
            self.voicebox.ask("test question", conversation_id="invalid-uuid")

    def test_stream_ask_standard_mode(self):
        """Test streaming with standard mode (multiple chunks)"""
        fake_stream_post, _ = self._mock_stream_post(STANDARD_MODE_EVENTS)

        with MagicMock(wraps=self.client) as mock_client:
            self.voicebox.client = mock_client
            mock_client._stream_post = MagicMock(side_effect=fake_stream_post)

            with self.voicebox.stream_ask("How many products?") as stream:
                results = list(stream)

        assert len(results) == 3
        # Intermediate events
        assert results[0].pending is True
        assert results[0].content == ""
        assert results[1].pending is True
        # Final event
        assert results[2].pending is False
        assert (
            results[2].content
            == "Based on the data in your knowledge graph, there are 157 products currently in the database."
        )
        assert results[2].conversation_id == "550e8400-e29b-41d4-a716-446655440000"
        assert (
            results[2].interpreted_question == "How many products are in the database?"
        )
        assert (
            results[2].sparql_query
            == "PREFIX : <http://example.org/>\nSELECT (COUNT(?product) AS ?count)\nWHERE {\n  ?product a :Product .\n}"
        )

    def test_stream_ask_with_conversation_id(self):
        """Test that conversation_id is passed in request body"""
        fake_stream_post, _ = self._mock_stream_post(SINGLE_NDJSON_EVENT)

        self.voicebox.client = MagicMock(wraps=self.client)
        self.voicebox.client._stream_post = MagicMock(side_effect=fake_stream_post)

        conv_id = "550e8400-e29b-41d4-a716-446655440000"
        with self.voicebox.stream_ask("test?", conversation_id=conv_id) as stream:
            list(stream)

        call_kwargs = self.voicebox.client._stream_post.call_args
        assert call_kwargs.kwargs["json"]["conversation_id"] == conv_id

    def test_stream_ask_with_auth_override(self):
        """Test that auth override header is sent"""
        fake_stream_post, _ = self._mock_stream_post(SINGLE_NDJSON_EVENT)

        self.voicebox.client = MagicMock(wraps=self.client)
        self.voicebox.client._stream_post = MagicMock(side_effect=fake_stream_post)

        with self.voicebox.stream_ask(
            "test?", stardog_auth_token_override="sso-token-12345"
        ) as stream:
            list(stream)

        call_kwargs = self.voicebox.client._stream_post.call_args
        assert call_kwargs.kwargs["headers"]["X-SD-Auth-Token"] == "sso-token-12345"

    def test_stream_ask_invalid_conversation_id(self):
        """Test that invalid conversation_id raises ValueError"""
        with pytest.raises(ValueError, match="conversation_id must be a valid UUID"):
            with self.voicebox.stream_ask(
                "test?", conversation_id="invalid-uuid"
            ) as stream:
                pass

    def test_stream_ask_no_client_id(self):
        """Test that missing client_id raises ValueError"""
        voicebox = VoiceboxApp(
            client=self.client,
            app_api_token="test-token",
            client_id=None,
        )
        with pytest.raises(ValueError, match="client_id required"):
            with voicebox.stream_ask("test?") as stream:
                pass

    def test_stream_ask_empty_lines_skipped(self):
        """Test that blank NDJSON lines are skipped"""
        mock_response = MagicMock()
        lines = ["", json.dumps(SINGLE_NDJSON_EVENT[0]), "", "  "]
        mock_response.iter_lines.return_value = iter(lines)

        @contextmanager
        def fake_stream_post(path, **kwargs):
            yield mock_response

        self.voicebox.client = MagicMock(wraps=self.client)
        self.voicebox.client._stream_post = MagicMock(side_effect=fake_stream_post)

        with self.voicebox.stream_ask("test?") as stream:
            results = list(stream)

        assert len(results) == 1

    def test_stream_ask_field_mapping(self):
        """Test that 'result' from NDJSON is mapped to 'content' on VoiceboxAnswer"""
        fake_stream_post, _ = self._mock_stream_post(SINGLE_NDJSON_EVENT)

        self.voicebox.client = MagicMock(wraps=self.client)
        self.voicebox.client._stream_post = MagicMock(side_effect=fake_stream_post)

        with self.voicebox.stream_ask("test?") as stream:
            results = list(stream)

        # The NDJSON has "result" but VoiceboxAnswer exposes it as "content"
        assert results[0].content == SINGLE_NDJSON_EVENT[0]["result"]

    def test_pending_none_for_non_streaming(self):
        """Test that non-streaming VoiceboxAnswer has pending=None (backward compat)"""
        from stardog.cloud.voicebox import VoiceboxAnswer

        answer = VoiceboxAnswer(
            content="test",
            conversation_id="abc",
            message_id="def",
        )
        assert answer.pending is None


class TestVoiceboxAppAsync:
    """Test VoiceboxApp with async client and Stardog Cloud API responses"""

    def setup_method(self):
        """Set up test fixtures"""
        self.client = AsyncClient()
        self.voicebox = VoiceboxApp(
            client=self.client,
            app_api_token="test-app-token",
            client_id="test-client-id",
        )

    def _mock_async_stream_post(self, ndjson_dicts):
        """Return an asynccontextmanager-patched _stream_post."""
        mock_response = _make_async_mock_stream_response(ndjson_dicts)

        @asynccontextmanager
        async def fake_stream_post(path, **kwargs):
            yield mock_response

        return fake_stream_post, mock_response

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_ask(self):
        """Test async ask method"""
        api_response = {
            "result": "Customer satisfaction averages 4.2/5 across all categories.",
            "conversation_id": "b8c7d6e5-f4a3-2910-8765-432109876543",
            "message_id": "e3f2a1b0-c9d8-7654-3210-fedcba987654",
            "actions": [
                {
                    "type": "sparql",
                    "value": "SELECT ?category (AVG(?rating) AS ?avg_rating) WHERE { ?review :category ?category ; :rating ?rating . } GROUP BY ?category",
                }
            ],
        }

        mock_request = respx.post(f"{self.client.base_url}/v1/voicebox/ask").mock(
            return_value=httpx.Response(200, json=api_response)
        )

        result = await self.voicebox.async_ask(
            "How satisfied are customers?",
            stardog_auth_token_override="sd-token-override",
        )

        # Test end user model
        assert result.content == api_response["result"]
        assert result.sparql_query == api_response["actions"][0]["value"]
        assert len(result.actions) == 1
        # Verify auth header
        assert (
            mock_request.calls.last.request.headers["X-SD-Auth-Token"]
            == "sd-token-override"
        )

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_generate_query(self):
        """Test async generate_query method"""
        api_response = {
            "result": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\\nSELECT ?project ?title ?budget\\nWHERE {\\n  ?project a :Project ;\\n           rdfs:label ?title ;\\n           :budget ?budget .\\n  FILTER(?budget > 100000)\\n} ORDER BY DESC(?budget)",
            "conversation_id": "c4d3e2f1-a0b9-8765-4321-0987654321ab",
            "message_id": "d5e4f3a2-b1c0-9876-5432-1098765432bc",
            "actions": [
                {
                    "type": "rewritten_query",
                    "label": "Query Understanding",
                    "value": "Find high-budget projects with their titles and budget amounts",
                },
                {
                    "type": "sparql",
                    "label": "Generated Query",
                    "value": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\\nSELECT ?project ?title ?budget\\nWHERE {\\n  ?project a :Project ;\\n           rdfs:label ?title ;\\n           :budget ?budget .\\n  FILTER(?budget > 100000)\\n} ORDER BY DESC(?budget)",
                },
                {
                    "type": "csv",
                    "label": "Export Format",
                    "value": "project,title,budget\\nproj1,New Website,150000\\nproj2,Mobile App,120000",
                },
            ],
        }

        mock_request = respx.post(
            f"{self.client.base_url}/v1/voicebox/generate-query"
        ).mock(return_value=httpx.Response(200, json=api_response))

        result = await self.voicebox.async_generate_query(
            "Show me projects with large budgets",
            stardog_auth_token_override="async-query-token",
        )

        # Test end user model
        assert result.content == api_response["result"]
        assert result.conversation_id == api_response["conversation_id"]
        assert result.message_id == api_response["message_id"]
        assert result.interpreted_question == api_response["actions"][0]["value"]
        assert result.sparql_query == api_response["actions"][1]["value"]
        assert len(result.actions) == len(api_response["actions"])

        csv_action = next(a for a in result.actions if a.type == "csv")
        assert csv_action.value == api_response["actions"][2]["value"]

        # Verify auth header
        assert (
            mock_request.calls.last.request.headers["X-SD-Auth-Token"]
            == "async-query-token"
        )

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_settings(self):
        """Test async settings method"""
        api_response = {
            "name": "Production Analytics Bot",
            "database": "production-analytics",
            "model": "model_v2",
            "named_graphs": [
                "http://company.com/data",
            ],
            "reasoning": False,
        }

        respx.get(f"{self.client.base_url}/v1/app").mock(
            return_value=httpx.Response(200, json=api_response)
        )

        settings = await self.voicebox.async_settings()

        # Test end user model
        assert settings.name == api_response["name"]
        assert settings.database == api_response["database"]
        assert settings.model == api_response["model"]
        assert len(settings.named_graphs) == 1
        assert "http://company.com/data" in settings.named_graphs
        assert not settings.reasoning

    @pytest.mark.asyncio
    async def test_async_stream_ask_standard_mode(self):
        """Test async streaming with standard mode (multiple chunks)"""
        fake_stream_post, _ = self._mock_async_stream_post(STANDARD_MODE_EVENTS)

        self.voicebox.client = MagicMock(wraps=self.client)
        self.voicebox.client._stream_post = MagicMock(side_effect=fake_stream_post)

        results = []
        async with self.voicebox.async_stream_ask("How many products?") as stream:
            async for answer in stream:
                results.append(answer)

        assert len(results) == 3
        assert results[0].pending is True
        assert results[0].content == ""
        assert results[2].pending is False
        assert (
            results[2].content
            == "Based on the data in your knowledge graph, there are 157 products currently in the database."
        )
        assert (
            results[2].interpreted_question == "How many products are in the database?"
        )
        assert (
            results[2].sparql_query
            == "PREFIX : <http://example.org/>\nSELECT (COUNT(?product) AS ?count)\nWHERE {\n  ?product a :Product .\n}"
        )
