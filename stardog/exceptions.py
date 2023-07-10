class StardogException(Exception):
    """General Stardog Exceptions"""

    def __init__(self, message: str, http_code: int = None, stardog_code: str = None):
        self.http_code = http_code
        self.stardog_code = stardog_code

        super().__init__(message)


class TransactionException(StardogException):
    """Transaction Exceptions"""
