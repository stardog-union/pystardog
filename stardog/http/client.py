import requests
import requests.auth
import requests_toolbelt.multipart as multipart

from .. import exceptions as exceptions


class Client:
    DEFAULT_ENDPOINT = "http://localhost:5820"
    DEFAULT_USERNAME = "admin"
    DEFAULT_PASSWORD = "admin"

    def __init__(
        self,
        endpoint=None,
        database=None,
        username=None,
        password=None,
        session=None,
        auth=None,
        run_as=None,
    ):
        self.url = endpoint if endpoint else self.DEFAULT_ENDPOINT

        # XXX this might not be right when the auth object is used.  Ideally we could drop storing this
        # information with this object but it is used when a store procedure is made as the "creator"
        self.username = username if username else self.DEFAULT_USERNAME

        if database:
            self.url = "{}/{}".format(self.url, database)

        if session is None:
            self.session = requests.Session()
        elif isinstance(session, requests.Session):
            # allows using e.g. proxy configuration defined explicitly
            # besides standard environment variables like http_proxy, https_proxy, no_proxy and curl_ca_bundle
            self.session = session
        else:
            raise TypeError(
                f"type(session) = {type(session)} must be a valid requests.Session object."
            )

        if auth is None:
            auth = requests.auth.HTTPBasicAuth(
                self.username, password if password else self.DEFAULT_PASSWORD
            )
        self.session.auth = auth

        if run_as:
            self.session.headers.update({"SD-Run-As": run_as})

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
                msg = {"message": request.text}

            raise exceptions.StardogException(
                "[{}] {}: {}".format(
                    request.status_code, msg.get("code", ""), msg.get("message", "")
                ),
                request.status_code,
                msg.get("code"),
            )

        return request

    def _multipart(self, response):
        decoder = multipart.decoder.MultipartDecoder.from_response(response)
        return [part.content for part in decoder.parts]
