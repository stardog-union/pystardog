from unittest.mock import Mock, patch
import uuid

import httpx
import pytest

from stardog.cloud.voicebox import VoiceboxApp


class MockClient:
    """Mock client for testing VoiceboxApp"""

    def __init__(self):
        self.responses = {}
        self.last_call = None

    def _post(self, path: str, **kwargs):
        """Mock POST method"""
        self.last_call = {"method": "POST", "path": path, "kwargs": kwargs}
        return self.responses.get(path, Mock())

    def _get(self, path: str, **kwargs):
        """Mock GET method"""
        self.last_call = {"method": "GET", "path": path, "kwargs": kwargs}
        return self.responses.get(path, Mock())

    def set_response(self, path: str, response_data: dict, status_code: int = 200):
        """Set mock response for a specific path"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.json.return_value = response_data
        mock_response.status_code = status_code
        self.responses[path] = mock_response


class TestVoiceboxApp:
    """Test VoiceboxApp with Stardog Cloud API responses"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_client = MockClient()
        self.voicebox = VoiceboxApp(
            client=self.mock_client,
            app_api_token="test-app-token",
            client_id="test-client-id",
        )

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
                    "value": "PREFIX : <http://example.org/>\nSELECT (COUNT(?product) AS ?count)\nWHERE {\n  ?product a :Product .\n}",
                },
            ],
        }

        self.mock_client.set_response("/v1/voicebox/ask", api_response)

        result = self.voicebox.ask("How many products are there?")

        # Test end user model
        assert result.content == api_response["result"]
        assert result.conversation_id == api_response["conversation_id"]
        assert result.message_id == api_response["message_id"]
        assert result.interpreted_question == api_response["actions"][0]["value"]
        assert result.sparql_query == api_response["actions"][1]["value"]
        assert len(result.actions) == len(api_response["actions"])
        assert result.actions[1].type == api_response["actions"][1]["type"]

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

        with patch.object(self.mock_client, "_post") as mock_post:
            mock_response = Mock(spec=httpx.Response)
            mock_response.json.return_value = api_response
            mock_post.return_value = mock_response

            result = self.voicebox.ask(
                "What were Q4 sales?", stardog_auth_token_override="sso-token-12345"
            )

            # Verify auth header
            headers = mock_post.call_args[1]["headers"]
            assert headers["X-SD-Auth-Token"] == "sso-token-12345"

            # Test end user model
            assert result.content == api_response["result"]
            assert result.conversation_id == api_response["conversation_id"]
            assert result.message_id == api_response["message_id"]
            assert result.actions[0].type == api_response["actions"][0]["type"]
            assert result.actions[0].value == api_response["actions"][0]["value"]
            assert result.sparql_query == api_response["actions"][0]["value"]

    def test_generate_query(self):
        """Test generate_query method"""
        api_response = {
            "result": "PREFIX ex: <http://example.org/>\nSELECT ?person ?name ?age\nWHERE {\n  ?person a ex:Employee ;\n          ex:name ?name ;\n          ex:age ?age .\n  FILTER(?age > 30)\n}",
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
                    "value": "PREFIX ex: <http://example.org/>\nSELECT ?person ?name ?age\nWHERE {\n  ?person a ex:Employee ;\n          ex:name ?name ;\n          ex:age ?age .\n  FILTER(?age > 30)\n}",
                },
            ],
        }

        self.mock_client.set_response("/v1/voicebox/generate-query", api_response)

        result = self.voicebox.generate_query("Show me employees over 30")

        # Test end user model
        assert result.content == api_response["result"]
        assert result.conversation_id == api_response["conversation_id"]
        assert result.message_id == api_response["message_id"]
        assert result.interpreted_question == api_response["actions"][0]["value"]
        assert result.sparql_query == api_response["actions"][1]["value"]
        assert len(result.actions) == len(api_response["actions"])

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

        self.mock_client.set_response("/v1/app", api_response)

        settings = self.voicebox.settings()

        # Test end user model
        assert settings.name == api_response["name"]
        assert settings.database == api_response["database"]
        assert settings.model == api_response["model"]
        assert settings.named_graphs == api_response["named_graphs"]
        assert settings.reasoning == api_response["reasoning"]

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

        async def mock_async_response():
            mock_response = Mock(spec=httpx.Response)
            mock_response.json.return_value = api_response
            return mock_response

        with patch.object(self.mock_client, "_post") as mock_post:
            mock_post.return_value = mock_async_response()

            result = await self.voicebox.async_ask(
                "How satisfied are customers?",
                stardog_auth_token_override="sd-token-override",
            )

            # Test end user model
            assert result.content == api_response["result"]
            assert result.sparql_query == api_response["actions"][0]["value"]
            assert len(result.actions) == 1
            # Verify auth header
            headers = mock_post.call_args[1]["headers"]
            assert headers["X-SD-Auth-Token"] == "sd-token-override"

    @pytest.mark.asyncio
    async def test_async_generate_query(self):
        """Test async generate_query method"""
        api_response = {
            "result": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT ?project ?title ?budget\nWHERE {\n  ?project a :Project ;\n           rdfs:label ?title ;\n           :budget ?budget .\n  FILTER(?budget > 100000)\n} ORDER BY DESC(?budget)",
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
                    "value": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT ?project ?title ?budget\nWHERE {\n  ?project a :Project ;\n           rdfs:label ?title ;\n           :budget ?budget .\n  FILTER(?budget > 100000)\n} ORDER BY DESC(?budget)",
                },
                {
                    "type": "csv",
                    "label": "Export Format",
                    "value": "project,title,budget\nproj1,New Website,150000\nproj2,Mobile App,120000",
                },
            ],
        }

        async def mock_async_response():
            mock_response = Mock(spec=httpx.Response)
            mock_response.json.return_value = api_response
            return mock_response

        with patch.object(self.mock_client, "_post") as mock_post:
            mock_post.return_value = mock_async_response()

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
            headers = mock_post.call_args[1]["headers"]
            assert headers["X-SD-Auth-Token"] == "async-query-token"

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

        async def mock_async_response():
            mock_response = Mock(spec=httpx.Response)
            mock_response.json.return_value = api_response
            return mock_response

        with patch.object(self.mock_client, "_get") as mock_get:
            mock_get.return_value = mock_async_response()

            settings = await self.voicebox.async_settings()

            # Test end user model
            assert settings.name == api_response["name"]
            assert settings.database == api_response["database"]
            assert settings.model == api_response["model"]
            assert len(settings.named_graphs) == 1
            assert "http://company.com/data" in settings.named_graphs
            assert not settings.reasoning

    def test_ask_with_invalid_conversation_id(self):
        """Test that invalid conversation_id raises ValueError"""
        with pytest.raises(
            ValueError,
            match="conversation_id must be a valid UUID format, got: invalid-uuid",
        ):
            self.voicebox.ask("test question", conversation_id="invalid-uuid")
