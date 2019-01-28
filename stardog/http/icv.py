import distutils.util


class ICV(object):
    def __init__(self, conn):
        self.client = conn.client

    def add(
            self,
            content,
            content_type,
            content_encoding=None,
    ):
        self.client.post(
            '/icv/add',
            data=content,
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
        )

    def remove(
            self,
            content,
            content_type,
            content_encoding=None,
    ):
        self.client.post(
            '/icv/remove',
            data=content,
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
        )

    def clear(self):
        self.client.post('/icv/clear')

    def is_valid(self,
                 content,
                 content_type,
                 content_encoding=None,
                 transaction=None,
                 graph_uri=None):
        url = 'icv/{}/validate'.format(
            transaction) if transaction else '/icv/validate'

        r = self.client.post(
            url,
            data=content,
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
            params={'graph-uri': graph_uri},
        )

        return bool(distutils.util.strtobool(r.text))

    def explain_violations(self,
                           content,
                           content_type,
                           content_encoding=None,
                           transaction=None,
                           graph_uri=None):
        url = '/icv/{}/violations'.format(
            transaction) if transaction else '/icv/violations'

        r = self.client.post(
            url,
            data=content,
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
            params={'graph-uri': graph_uri},
        )

        return self.client._multipart(r)

    def convert(self,
                content,
                content_type,
                content_encoding=None,
                graph_uri=None):
        r = self.client.post(
            '/icv/convert',
            data=content,
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
            params={'graph-uri': graph_uri},
        )

        return r.text
