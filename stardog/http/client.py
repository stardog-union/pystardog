import requests
import requests_toolbelt.multipart as multipart

from .. import exceptions as exceptions


class Client(object):

    DEFAULT_ENDPOINT = 'http://localhost:5820'
    DEFAULT_USERNAME = 'admin'
    DEFAULT_PASSWORD = 'admin'
    # The maximum number of connections to save in the pool.
    DEFAULT_POOL_SIZE = 10

    def __init__(self,
                 endpoint=None,
                 database=None,
                 username=None,
                 password=None,
                 pool_size=None):
        self.url = endpoint if endpoint else self.DEFAULT_ENDPOINT
        self.username = username if username else self.DEFAULT_USERNAME
        self.password = password if password else self.DEFAULT_PASSWORD
        self.pool_size = pool_size if pool_size else self.DEFAULT_POOL_SIZE

        if database:
            self.url = '{}/{}'.format(self.url, database)

        adapter = requests.adapters.HTTPAdapter(pool_maxsize=self.pool_size)
        self.session = requests.Session()
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.session.auth = (self.username, self.password)

    def post(self, path, **kwargs):
        return self.__wrap(self.session.post(self.url + path, **kwargs))

    def put(self, path, **kwargs):
        return self.__wrap(self.session.put(self.url + path, **kwargs))

    def get(self, path, **kwargs):
        return self.__wrap(self.session.get(self.url + path, **kwargs))

    def delete(self, path, **kwargs):
        return self.__wrap(self.session.delete(self.url + path, **kwargs))

    def close(self):
        self.session.close()

    def __wrap(self, request):
        if not request.ok:
            try:
                msg = request.json()
            except ValueError:
                # sometimes errors come as strings
                msg = {'message': request.text}

            raise exceptions.StardogException('[{}] {}: {}'.format(
                request.status_code, msg.get('code', ''), msg.get(
                    'message', '')))
        return request

    def _multipart(self, response):
        decoder = multipart.decoder.MultipartDecoder.from_response(response)
        return [part.content for part in decoder.parts]
