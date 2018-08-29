from contextlib import contextmanager

from stardog.http.connection import Connection as HTTPConnection
from stardog.exceptions import TransactionException
from stardog.content_types import SPARQL_JSON, TURTLE, BOOLEAN

class Connection(object):

    def __init__(self, database, endpoint=None, username=None, password=None):
        self.conn = HTTPConnection(database, endpoint, username, password)
        self.transaction = None

    def docs(self):
        return Docs(self)

    def icv(self):
        return ICV(self)

    def versioning(self):
        return VCS(self)
    
    def begin(self):
        self._assert_not_in_transaction()
        self.transaction = self.conn.begin()

    def rollback(self):
        self._assert_in_transaction()
        self.conn.rollback(self.transaction)
        self.transaction = None

    def commit(self):
        self._assert_in_transaction()
        self.conn.commit(self.transaction)
        self.transaction = None

    def add(self, content, graph_uri=None):
        self._assert_in_transaction()

        with content.data() as data:
            self.conn.add(self.transaction, content.content_type, data, graph_uri)
    
    def remove(self, content, graph_uri=None):
        self._assert_in_transaction()

        with content.data() as data:
            self.conn.remove(self.transaction, content.content_type, data, graph_uri)
    
    def clear(self, graph_uri=None):
        self._assert_in_transaction()
        self.conn.clear(self.transaction, graph_uri)

    def size(self):
        return self.conn.size()

    def export(self, content_type=TURTLE, stream=False, chunk_size=10240):
        db = self.conn.export(content_type, stream, chunk_size)
        return nextcontext(db) if stream else next(db)
    
    def explain(self, query, base_uri=None):
        return self.conn.explain(query, base_uri)

    def select(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=SPARQL_JSON):
        return self.conn.query(query, self.transaction, base_uri, limit, offset, timeout, reasoning, bindings, content_type)

    def graph(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=TURTLE):
        return self.conn.query(query, self.transaction, base_uri, limit, offset, timeout, reasoning, bindings, content_type)

    def paths(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=SPARQL_JSON):
        return self.conn.query(query, self.transaction, base_uri, limit, offset, timeout, reasoning, bindings, content_type)

    def ask(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None):
        r = self.conn.query(query, self.transaction, base_uri, limit, offset, timeout, reasoning, bindings, BOOLEAN)
        return bool(r)

    def update(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None):
        self.conn.update(query, self.transaction, base_uri, limit, offset, timeout, reasoning)

    def is_consistent(self, graph_uri=None):
        return self.conn.is_consistent(graph_uri)

    def explain_inference(self, content):
        with content.data() as data:
            return self.conn.explain_inference(content.content_type, data, self.transaction)

    def explain_inconsistency(self, graph_uri=None):
        return self.conn.explain_inconsistency(self.transaction, self.graph_uri)

    def _assert_not_in_transaction(self):
        if self.transaction:
            raise TransactionException('Already in a transaction')
    
    def _assert_in_transaction(self):
        if not self.transaction:
            raise TransactionException('Not in a transaction')

class Docs(object):

    def __init__(self, conn):
        self.docs = conn.conn.docs()
    
    def size(self):
        return self.docs.size()

    def add(self, name, content):
        with content.data() as data:
            self.docs.add(name, data)

    def clear(self):
        self.docs.clear()
    
    def get(self, name, stream=False, chunk_size=10240):
        doc = self.docs.get(name, stream, chunk_size)
        return nextcontext(doc) if stream else next(doc)

    def delete(self, name):
        self.docs.delete(name)

class ICV(object):

    def __init__(self, conn):
        self.conn = conn
        self.icv = conn.conn.icv()
    
    def add(self, content):
        with content.data() as data:
            self.icv.add(content.content_type, data)

    def remove(self, content):
        with content.data() as data:
            self.icv.remove(content.content_type, data)
    
    def clear(self):
        self.icv.clear()
    
    def is_valid(self, content, graph_uri=None):
        with content.data() as data:
            return self.icv.is_valid(content.content_type, data, self.conn.transaction, graph_uri)

    def explain_violations(self, content, graph_uri=None):
        with content.data() as data:
            return self.icv.explain_violations(content.content_type, data, self.conn.transaction, graph_uri)

    def convert(self, content, graph_uri=None):
        with content.data() as data:
            return self.icv.convert(content.content_type, data, graph_uri)

class VCS(object):

    def __init__(self, conn):
        self.vcs = conn.conn.versioning()
        self.conn = conn

    def select(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=SPARQL_JSON):
        return self.vcs.query(query, base_uri, limit, offset, timeout, reasoning, bindings, content_type)

    def graph(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=TURTLE):
        return self.vcs.query(query, base_uri, limit, offset, timeout, reasoning, bindings, content_type)

    def paths(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=SPARQL_JSON):
        return self.vcs.query(query, base_uri, limit, offset, timeout, reasoning, bindings, content_type)

    def ask(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None):
        r = self.vcs.query(query, base_uri, limit, offset, timeout, reasoning, bindings, BOOLEAN)
        return bool(r)

    def commit(self, message):
        self.conn._assert_in_transaction()
        self.vcs.commit(self.conn.transaction, message)
    
    def create_tag(self, revision, name):
        self.vcs.create_tag(revision, name)

    def delete_tag(self, name):
        self.vcs.delete_tag(name)

    def revert(self, to_revision, from_revision, message):
        self.vcs.revert(to_revision, from_revision, message)


@contextmanager
def nextcontext(r):
    yield next(r)