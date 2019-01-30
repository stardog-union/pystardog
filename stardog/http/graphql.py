from .. import exceptions as exceptions


class GraphQL(object):
    def __init__(self, conn):
        self.client = conn.client

    def query(self, query, variables=None):
        r = self.client.post(
            '/graphql',
            json={
                'query': query,
                'variables': variables if variables else {}
            })

        res = r.json()
        if 'data' in res:
            return res['data']

        # graphql endpoint returns valid response with errors
        raise exceptions.StardogException(res)

    def schemas(self):
        r = self.client.get('/graphql/schemas')
        return r.json()['schemas']

    def clear_schemas(self):
        self.client.delete('/graphql/schemas')

    def add_schema(self, name, schema):
        self.client.put('/graphql/schemas/{}'.format(name), data=schema)

    def schema(self, name):
        r = self.client.get('/graphql/schemas/{}'.format(name))
        return r.text

    def remove_schema(self, name):
        self.client.delete('/graphql/schemas/{}'.format(name))
