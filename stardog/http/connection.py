import distutils.util

from .. import content_types as content_types
from . import client as http_client
from . import docs as http_docs
from . import graphql as http_graphql
from . import icv as http_icv


class Connection(object):
    def __init__(self, database, endpoint=None, username=None, password=None):
        self.client = http_client.Client(endpoint, database, username,
                                         password)

    def docs(self):
        return http_docs.Docs(self)

    def icv(self):
        return http_icv.ICV(self)

    def graphql(self):
        return http_graphql.GraphQL(self)

    def begin(self, **kwargs):
        r = self.client.post('/transaction/begin', params=kwargs)
        return r.text

    def rollback(self, transaction):
        self.client.post('/transaction/rollback/{}'.format(transaction))

    def commit(self, transaction):
        self.client.post('/transaction/commit/{}'.format(transaction))

    def add(self,
            transaction,
            content,
            content_type,
            content_encoding=None,
            graph_uri=None):
        self.client.post(
            '/{}/add'.format(transaction),
            params={'graph-uri': graph_uri},
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
            data=content)

    def remove(self,
               transaction,
               content,
               content_type,
               content_encoding=None,
               graph_uri=None):
        self.client.post(
            '/{}/remove'.format(transaction),
            params={'graph-uri': graph_uri},
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
            data=content)

    def clear(self, transaction, graph_uri=None):
        self.client.post(
            '/{}/clear'.format(transaction), params={'graph-uri': graph_uri})

    def size(self, exact=False):
        r = self.client.get('/size', params={'exact': exact})
        return int(r.text)

    def export(self,
               content_type=content_types.TURTLE,
               stream=False,
               chunk_size=10240):
        with self.client.get(
                '/export', headers={'Accept': content_type},
                stream=stream) as r:
            yield r.iter_content(
                chunk_size=chunk_size) if stream else r.content

    def query(self,
              query,
              transaction=None,
              content_type=content_types.SPARQL_JSON,
              **kwargs):
        r = self.__query(query, 'query', transaction, content_type, **kwargs)
        return r.json(
        ) if content_type == content_types.SPARQL_JSON else r.content

    def update(self, query, transaction=None, **kwargs):
        self.__query(query, 'update', transaction, None, **kwargs)

    def __query(self,
                query,
                method,
                transaction=None,
                content_type=None,
                **kwargs):
        params = {
            'query': query,
            'baseURI': kwargs.get('base_uri'),
            'limit': kwargs.get('limit'),
            'offset': kwargs.get('offset'),
            'timeout': kwargs.get('timeout'),
            'reasoning': kwargs.get('reasoning')
        }

        # query bindings
        bindings = kwargs.get('bindings', {})
        for k, v in bindings.items():
            params['${}'.format(k)] = v

        url = '/{}/{}'.format(transaction,
                              method) if transaction else '/{}'.format(method)

        return self.client.post(
            url,
            data=params,
            headers={'Accept': content_type},
        )

    def explain(self, query, base_uri=None):
        params = {'query': query, 'baseURI': base_uri}

        r = self.client.post(
            '/explain',
            data=params,
        )

        return r.text

    def is_consistent(self, graph_uri=None):
        r = self.client.get(
            '/reasoning/consistency',
            params={'graph-uri': graph_uri},
        )

        return bool(distutils.util.strtobool(r.text))

    def explain_inference(self,
                          content,
                          content_type,
                          content_encoding=None,
                          transaction=None):
        url = '/reasoning/{}/explain'.format(
            transaction) if transaction else '/reasoning/explain'

        r = self.client.post(
            url,
            data=content,
            headers={
                'Content-Type': content_type,
                'Content-Encoding': content_encoding
            },
        )

        return r.json()['proofs']

    def explain_inconsistency(self, transaction=None, graph_uri=None):
        url = '/reasoning/{}/explain/inconsistency'.format(
            transaction) if transaction else '/reasoning/explain/inconsistency'

        r = self.client.get(
            url,
            params={'graph-uri': graph_uri},
        )

        return r.json()['proofs']

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()
