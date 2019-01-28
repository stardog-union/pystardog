class Docs(object):
    def __init__(self, conn):
        self.client = conn.client

    def size(self):
        r = self.client.get('/docs/size')
        return int(r.text)

    def add(self, name, content):
        self.client.post('/docs', files={'upload': (name, content)})

    def clear(self):
        self.client.delete('/docs')

    def get(self, name, stream=False, chunk_size=10240):
        with self.client.get('/docs/{}'.format(name), stream=stream) as r:
            yield r.iter_content(
                chunk_size=chunk_size) if stream else r.content

    def delete(self, name):
        self.client.delete('/docs/{}'.format(name))
