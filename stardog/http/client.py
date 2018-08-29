from requests import Session, Request
from requests_toolbelt.multipart import decoder

from stardog.exceptions import StardogException

class Client(object):

    DEFAULT_ENDPOINT = 'http://localhost:5820'

    def __init__(self, endpoint=None, database=None, username=None, password=None):
        self.url = endpoint if endpoint else self.DEFAULT_ENDPOINT

        if database:
            self.url = '{}/{}'.format(self.url, database)

        self.session = Session()

        if username and password:
            self.session.auth = (username, password)

    def post(self, path, **kwargs):
        return self.__wrap(self.session.post(self.url + path, **kwargs))

    def put(self, path, **kwargs):
        return self.__wrap(self.session.put(self.url + path, **kwargs))

    def get(self, path, **kwargs):
        return self.__wrap(self.session.get(self.url + path, **kwargs))

    def delete(self, path, **kwargs):
        return self.__wrap(self.session.delete(self.url + path, **kwargs))

    def __wrap(self, request):
        if not request.ok:
            try:
                msg = request.json()
            except ValueError:
                # sometimes errors come as strings
                msg = {'message': request.text}

            raise StardogException('[{}] {}: {}'.format(request.status_code, msg.get('code', ''), msg.get('message', '')))
        return request

    def _multipart(self, response):
        multipart = decoder.MultipartDecoder.from_response(response)
        return [part.content for part in multipart.parts]
