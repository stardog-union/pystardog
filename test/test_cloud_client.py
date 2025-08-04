import pytest
from unittest.mock import Mock, patch
import httpx
import json

from stardog.cloud.client import Client, AsyncClient, StardogCloudAPIEndpoints
from stardog.cloud.exceptions import (
    BadRequestException,
    UnauthorizedException,
    ForbiddenException,
    NotFoundException,
    RateLimitExceededException,
    InternalServerException,
    GatewayTimeoutException,
    StardogCloudException,
)


class TestClientErrorHandling:
    """Test Client error response handling"""

    def setup_method(self):
        """Set up test fixtures"""
        self.client = Client()

    def teardown_method(self):
        """Clean up after tests"""
        self.client.close()

    def test_handle_response_400_bad_request(self):
        """Test 400 Bad Request error handling"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 400
        mock_response.json.return_value = {"message": "Invalid request parameters"}

        with pytest.raises(BadRequestException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Invalid request parameters"
        assert exc_info.value.status_code == 400

    def test_handle_response_401_unauthorized(self):
        """Test 401 Unauthorized error handling"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 401
        mock_response.json.return_value = {"detail": "Authentication required"}

        with pytest.raises(UnauthorizedException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Authentication required"
        assert exc_info.value.status_code == 401

    def test_handle_response_403_forbidden(self):
        """Test 403 Forbidden error handling"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 403
        mock_response.json.return_value = {"message": "Access denied"}

        with pytest.raises(ForbiddenException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Access denied"
        assert exc_info.value.status_code == 403

    def test_handle_response_404_not_found(self):
        """Test 404 Not Found error handling"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 404
        mock_response.json.return_value = {"message": "Resource not found"}

        with pytest.raises(NotFoundException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Resource not found"
        assert exc_info.value.status_code == 404

    def test_handle_response_429_rate_limit(self):
        """Test 429 Rate Limit error handling"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 429
        mock_response.json.return_value = {"message": "Rate limit exceeded"}

        with pytest.raises(RateLimitExceededException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Rate limit exceeded"
        assert exc_info.value.status_code == 429

    def test_handle_response_500_internal_server_error(self):
        """Test 500 Internal Server Error handling"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 500
        mock_response.json.return_value = {"message": "Internal server error"}

        with pytest.raises(InternalServerException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Internal server error"
        assert exc_info.value.status_code == 500

    def test_handle_response_504_gateway_timeout(self):
        """Test 504 Gateway Timeout error handling"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 504
        mock_response.json.return_value = {"message": "Gateway timeout"}

        with pytest.raises(GatewayTimeoutException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Gateway timeout"
        assert exc_info.value.status_code == 504

    def test_handle_response_unknown_error_code(self):
        """Test unknown error code falls back to base exception"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 418  # I'm a teapot
        mock_response.json.return_value = {"message": "I'm a teapot"}

        with pytest.raises(StardogCloudException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "I'm a teapot"
        assert exc_info.value.status_code == 418

    def test_handle_response_invalid_json(self):
        """Test error response with invalid JSON"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 400
        mock_response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)
        mock_response.text = "Invalid JSON response"

        with pytest.raises(BadRequestException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message == "Invalid JSON response"
        assert exc_info.value.status_code == 400

    def test_handle_response_empty_message(self):
        """Test error response with empty message"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 400
        mock_response.json.return_value = {"message": ""}

        with pytest.raises(BadRequestException) as exc_info:
            self.client._handle_response(mock_response)

        # Empty string evaluates to None in the error handling logic
        assert exc_info.value.message is None
        assert exc_info.value.status_code == 400

    def test_handle_response_no_message_field(self):
        """Test error response with no message or detail field"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = True
        mock_response.status_code = 400
        mock_response.json.return_value = {"error": "something went wrong"}

        with pytest.raises(BadRequestException) as exc_info:
            self.client._handle_response(mock_response)

        assert exc_info.value.message is None
        assert exc_info.value.status_code == 400

    def test_handle_response_success(self):
        """Test successful response passes through unchanged"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = False
        mock_response.status_code = 200

        result = self.client._handle_response(mock_response)

        assert result is mock_response


class TestClientBasicFunctionality:
    """Test basic Client functionality"""

    def test_client_initialization_default_endpoint(self):
        """Test client initializes with default US endpoint"""
        client = Client()
        assert client.base_url == StardogCloudAPIEndpoints.US.value
        client.close()

    def test_client_initialization_custom_endpoint(self):
        """Test client initializes with custom endpoint"""
        client = Client(base_url=StardogCloudAPIEndpoints.EU)
        assert client.base_url == StardogCloudAPIEndpoints.EU.value
        client.close()

    def test_client_initialization_custom_timeout(self):
        """Test client initializes with custom timeout"""
        client = Client(timeout=60.0)
        assert client._client.timeout.connect == 60.0
        client.close()

    def test_client_context_manager(self):
        """Test client as context manager"""
        with Client() as client:
            assert client._client is not None

    def test_voicebox_app_creation(self):
        """Test voicebox app creation"""
        client = Client()
        voicebox = client.voicebox_app("test-token", "test-client-id")

        assert voicebox.app_api_token == "test-token"
        assert voicebox.client_id == "test-client-id"
        assert voicebox.client is client

        client.close()


class TestClientHTTPMethods:
    """Test Client HTTP methods with mocked responses"""

    def setup_method(self):
        """Set up test fixtures"""
        self.client = Client()

    def teardown_method(self):
        """Clean up after tests"""
        self.client.close()

    def test_post_method(self):
        """Test POST method calls _handle_response"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = False

        with patch.object(
            self.client._client, "post", return_value=mock_response
        ) as mock_post:
            result = self.client._post("/test", json={"key": "value"})

            mock_post.assert_called_once_with("/test", json={"key": "value"})
            assert result is mock_response

    def test_delete_method(self):
        """Test DELETE method calls _handle_response"""
        mock_response = Mock(spec=httpx.Response)
        mock_response.is_error = False

        with patch.object(
            self.client._client, "delete", return_value=mock_response
        ) as mock_delete:
            result = self.client._delete("/test")

            mock_delete.assert_called_once_with("/test")
            assert result is mock_response


class TestAsyncClientBasicFunctionality:
    """Test basic AsyncClient functionality"""

    @pytest.mark.asyncio
    async def test_async_client_initialization_default_endpoint(self):
        """Test async client initializes with default US endpoint"""
        client = AsyncClient()
        assert client.base_url == "https://cloud.stardog.com/api"
        await client.aclose()

    @pytest.mark.asyncio
    async def test_async_client_initialization_custom_timeout(self):
        """Test async client initializes with custom timeout"""
        client = AsyncClient(timeout=90.0)
        assert client._client.timeout.connect == 90.0
        await client.aclose()

    @pytest.mark.asyncio
    async def test_async_client_context_manager(self):
        """Test async client as context manager"""
        async with AsyncClient() as client:
            assert client._client is not None

    @pytest.mark.asyncio
    async def test_async_voicebox_app_creation(self):
        """Test async voicebox app creation"""
        client = AsyncClient()
        voicebox = client.voicebox_app("async-token", "async-client-id")

        assert voicebox.app_api_token == "async-token"
        assert voicebox.client_id == "async-client-id"
        assert voicebox.client is client

        await client.aclose()
