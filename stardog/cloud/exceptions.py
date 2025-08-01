class StardogCloudException(Exception):
    """Base exception of all exceptions raised by the ``stardog.cloud`` subpackage."""

    def __init__(self, message, status_code=None):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class BadRequestException(StardogCloudException):
    """Exception when Stardog Cloud API replies with a 400 Bad Request"""

    pass


class UnauthorizedException(StardogCloudException):
    """Exception when Stardog Cloud API replies with a 401 Unauthorized"""

    pass


class ForbiddenException(StardogCloudException):
    """Exception when Stardog Cloud API replies with a 403 Forbidden"""

    pass


class NotFoundException(StardogCloudException):
    """Exception when Stardog Cloud API replies with a 404 Not Found"""

    pass


class RateLimitExceededException(StardogCloudException):
    """Exception when Stardog Cloud API replies with a 429 Too Many Requests"""

    pass


class InternalServerException(StardogCloudException):
    """Exception when Stardog Cloud API replies with a 500 Internal Server Error"""

    pass


class GatewayTimeoutException(StardogCloudException):
    """Exception when Stardog Cloud API replies with a 504 Gateway Timeout Error"""

    pass


_API_STATUS_EXCEPTIONS = {
    400: BadRequestException,
    401: UnauthorizedException,
    403: ForbiddenException,
    404: NotFoundException,
    429: RateLimitExceededException,
    500: InternalServerException,
    504: GatewayTimeoutException,
}
