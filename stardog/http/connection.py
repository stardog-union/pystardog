import json
from contextlib import contextmanager

from stardog.docs import Docs
from stardog.client import Client
from stardog.icv import ICV
from stardog.vcs import VCS
from stardog.content_types import SPARQL_JSON, TURTLE

class Connection(object):

    def __init__(self, database, endpoint=None, username=None, password=None):
        self.client = Client(endpoint, database, username, password)
    
    def docs(self):
        return Docs(self)

    def icv(self):
        return ICV(self)

    def versioning(self):
        return VCS(self)

    def begin(self):
        r = self.client.post('/transaction/begin')
        return r.text

    def rollback(self, transaction):
        self.client.post('/transaction/rollback/{}'.format(transaction))

    def commit(self, transaction):
        self.client.post('/transaction/commit/{}'.format(transaction))
    
    def add(self, transaction, content_type, content, graph_uri=None):
        self.client.post(
            '/{}/add'.format(transaction),
            params={'graph-uri': graph_uri},
            headers={'Content-Type': content_type},
            data=content
        )
    
    def remove(self, transaction, content_type, content, graph_uri=None):
        self.client.post(
            '/{}/remove'.format(transaction),
            params={'graph-uri': graph_uri},
            headers={'Content-Type': content_type},
            data=content
        )
    
    def clear(self, transaction, graph_uri=None):
        self.client.post(
            '/{}/clear'.format(transaction),
            params={'graph-uri': graph_uri}
        )

    def size(self):
        r = self.client.get('/size')
        return long(r.text)
    
    def export(self, content_type=TURTLE, stream=False, chunk_size=10240):
        with self.client.get('/export', headers={'Accept': content_type}, stream=stream) as r:
            yield r.iter_content(chunk_size=chunk_size) if stream else r.content

    def query(self, query, transaction=None, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=SPARQL_JSON):
        r = self.__query(query, 'query', transaction, base_uri, limit, offset, timeout, reasoning, bindings, content_type)
        return r.json() if content_type == SPARQL_JSON else r.content

    def update(self, query, transaction=None, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None):
        self.__query(query, 'update', transaction, base_uri, limit, offset, timeout, reasoning, bindings)

    def __query(self, query, method, transaction=None, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=None):
        params = {
            'query': query,
            'baseURI': base_uri,
            'limit': limit,
            'offset': offset,
            'timeout': timeout,
            'reasoning': reasoning
        }

        # query bindings
        bindings = bindings if bindings else {}
        for k,v in bindings.iteritems():
            params['${}'.format(k)] = v

        url = '/{}/{}'.format(transaction, method) if transaction else '/{}'.format(method)

        return self.client.post(
            url,
            data=params,
            headers={'Accept': content_type},
        )

    def explain(self, query, base_uri=None):
        params = {
            'query': query,
            'baseURI': base_uri
        }

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

        return bool(r.text)

    def explain_inference(self, content_type, content, transaction=None):
        url = '/reasoning/{}/explain'.format(transaction) if transaction else '/reasoning/explain'

        r = self.client.post(
            url,
            data=content,
            headers={'Content-Type': content_type},
        )

        return r.json()['proofs']

    def explain_inconsistency(self, transaction=None, graph_uri=None):
        url = '/reasoning/{}/explain/inconsistency'.format(transaction) if transaction else '/reasoning/explain/inconsistency'

        r = self.client.get(
            url,
            params={'graph-uri': graph_uri},
        )

        return r.json()['proofs']
