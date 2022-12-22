class StardogException(Exception):
    """General Stardog Exceptions"""

    def __init__(self, message, http_code=None, stardog_code=None):
        self.http_code = http_code
        self.stardog_code = stardog_code

        super().__init__(message)


class TransactionException(StardogException):
    """Transaction Exceptions"""
