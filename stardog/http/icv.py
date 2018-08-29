class ICV(object):

    def __init__(self, conn):
        self.client = conn.client
    
    def add(self, content_type, content):
        self.client.post(
            '/icv/add',
            data=content,
            headers={'Content-Type': content_type},
        )

    def remove(self, content_type, content):
        self.client.post(
            '/icv/remove',
            data=content,
            headers={'Content-Type': content_type},
        )
    
    def clear(self):
        self.client.post('/icv/clear')
    
    def is_valid(self, content_type, content, transaction=None, graph_uri=None):
        url = 'icv/{}/validate'.format(transaction) if transaction else '/icv/validate'

        r = self.client.post(
            url,
            data=content,
            headers={'Content-Type': content_type},
            params={'graph-uri': graph_uri},
        )

        return bool(r.text)

    def explain_violations(self, content_type, content, transaction=None, graph_uri=None):
        url = '/icv/{}/violations'.format(transaction) if transaction else '/icv/violations'

        r = self.client.post(
            url,
            data=content,
            headers={'Content-Type': content_type},
            params={'graph-uri': graph_uri},
        )

        return self.client._multipart(r)

    def convert(self, content_type, content, graph_uri=None):
        r = self.client.post(
            '/icv/convert',
            data=content,
            headers={'Content-Type': content_type},
            params={'graph-uri': graph_uri},
        )

        return r.text
