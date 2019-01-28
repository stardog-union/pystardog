from contextlib import contextmanager
from distutils.util import strtobool

from stardog.content_types import BOOLEAN, SPARQL_JSON, TURTLE
from stardog.exceptions import TransactionException
from stardog.http.connection import Connection as HTTPConnection


class Connection(object):
    """
    Database Connection.

    This is the entry point for all user-related operations on a
    Stardog database
    """

    def __init__(self, database, endpoint=None, username=None, password=None):
        """
        Initialize a connection to a Stardog database

        Parameters
            database (str)
                Name of the database
            endpoint (str)
                Url of the server endpoint. Defaults to `http://localhost:5820`
            username (str)
                Username to use in the connection (optional)
            password (str)
                Password to use in the connection (optional)

        Example
            >> conn = Connection('db', endpoint='http://localhost:9999',
                                 username='admin', password='admin')
        """
        self.conn = HTTPConnection(database, endpoint, username, password)
        self.transaction = None

    def docs(self):
        """
        BITES: Document Storage
        """
        return Docs(self)

    def icv(self):
        """
        Integrity Constraint Validation
        """
        return ICV(self)

    def versioning(self):
        """
        Versioning
        """
        return VCS(self)

    def graphql(self):
        """
        GraphQL
        """
        return GraphQL(self)

    def begin(self):
        """
        Begin a transaction

        Returns
            (str)
                Transaction ID

        Raises
            TransactionException
                If already in a transaction
        """
        self._assert_not_in_transaction()
        self.transaction = self.conn.begin()
        return self.transaction

    def rollback(self):
        """
        Rollback current transaction

        Raises
            TransactionException
                If currently not in a transaction
        """
        self._assert_in_transaction()
        self.conn.rollback(self.transaction)
        self.transaction = None

    def commit(self):
        """
        Commit current transaction

        Raises
            TransactionException
                If currently not in a transaction
        """
        self._assert_in_transaction()
        self.conn.commit(self.transaction)
        self.transaction = None

    def add(self, content, graph_uri=None):
        """
        Add data to database

        Parameters
            content (Content)
                Data to add
            graph_uri (str)
                Named graph into which to add the data (optional)

        Raises
            TransactionException
                If currently not in a transaction

        Example
            >> conn.add(File('example.ttl'), graph_uri='urn:graph')
        """
        self._assert_in_transaction()

        with content.data() as data:
            self.conn.add(self.transaction, data, content.content_type,
                          content.content_encoding, graph_uri)

    def remove(self, content, graph_uri=None):
        """
        Remove data from database

        Parameters
            content (Content)
                Data to add
            graph_uri (str)
                Named graph from which to remove the data (optional)

        Raises
            TransactionException
                If currently not in a transaction

        Example
            >> conn.remove(File('example.ttl'), graph_uri='urn:graph')
        """

        self._assert_in_transaction()

        with content.data() as data:
            self.conn.remove(self.transaction, data, content.content_type,
                             content.content_encoding, graph_uri)

    def clear(self, graph_uri=None):
        """
        Remove all data from database or specific named graph

        Parameters
            graph_uri (str)
                Named graph from which to remove data (optional)

        Raises
            TransactionException
                If currently not in a transaction

        Example
            # clear a specific named graph
            >> conn.clear('urn:graph')

            # clear the whole database
            >> conn.clear()
        """
        self._assert_in_transaction()
        self.conn.clear(self.transaction, graph_uri)

    def size(self):
        """
        Database size

        Returns
            (int)
                Number of elements in database
        """
        return self.conn.size()

    def export(self, content_type=TURTLE, stream=False, chunk_size=10240):
        """
        Export contents of database

        Parameters
            content_type (str)
                RDF content type. Defaults to 'text/turtle'
            stream (bool)
                If results should come in chunks or as a whole.
                Defaults to False
            chunk_size (int)
                Number of bytes to read per chunk when streaming.
                Defaults to 10240

        Returns
            (str)
                If stream=False
            (gen)
                If stream=True

        Example
            # no streaming
            >> contents = conn.export()

            # streaming
            >> with conn.export(stream=True) as stream:
                contents = ''.join(stream)
        """
        db = self.conn.export(content_type, stream, chunk_size)
        return nextcontext(db) if stream else next(db)

    def explain(self, query, base_uri=None):
        """
        Explain evaluation of a SPARQL query

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)

        Returns
            (str)
                Query explanation
        """
        return self.conn.explain(query, base_uri)

    def select(self, query, content_type=SPARQL_JSON, **kwargs):
        """
        Execute a SPARQL select query

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)
            content_type (str)
                Content type for results.
                Defaults to 'application/sparql-results+json'

        Returns
            (dict)
                If content_type='application/sparql-results+json'
            (str)
                Other content types

        Example
            >> conn.select('select * {?s ?p ?o}',
                           offset=100, limit=100, reasoning=True)

            # bindings
            >> conn.select('select * {?s ?p ?o}', bindings={'o': '<urn:a>'})
        """
        return self.conn.query(
            query, self.transaction, content_type=content_type, **kwargs)

    def graph(self, query, content_type=TURTLE, **kwargs):
        """
        Execute a SPARQL graph query

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)
            content_type (str)
                Content type for results. Defaults to 'text/turtle'

        Returns
            (str)
                Results in format given by content_type

        Example
            >> conn.graph('construct {?s ?p ?o} where {?s ?p ?o}',
                          offset=100, limit=100, reasoning=True)

            # bindings
            >> conn.graph('construct {?s ?p ?o} where {?s ?p ?o}',
                          bindings={'o': '<urn:a>'})
        """
        return self.conn.query(query, self.transaction, content_type, **kwargs)

    def paths(self, query, content_type=SPARQL_JSON, **kwargs):
        """
        Execute a SPARQL paths query

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)
            content_type (str)
                Content type for results.
                Defaults to 'application/sparql-results+json'

        Returns
            (dict)
                If content_type='application/sparql-results+json'
            (str)
                Other content types

        Example
            >> conn.paths('paths start ?x = :subj end ?y = :obj via ?p',
                          reasoning=True)
        """
        return self.conn.query(query, self.transaction, content_type, **kwargs)

    def ask(self, query, **kwargs):
        """
        Execute a SPARQL ask query

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)

        Returns
            (bool)
                Result of ask query

        Example
            >> conn.ask('ask {:subj :pred :obj}', reasoning=True)
        """
        r = self.conn.query(query, self.transaction, BOOLEAN, **kwargs)
        return bool(strtobool(r.decode()))

    def update(self, query, **kwargs):
        """
        Execute a SPARQL update query

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)

        Example
            >> conn.update('delete where {?s ?p ?o}')
        """
        self.conn.update(query, self.transaction, **kwargs)

    def is_consistent(self, graph_uri=None):
        """
        Check if database or specific named graph is consistent wrt its schema

        Parameters
            graph_uri (str)
                Named graph from which to check consistency (optional)

        Returns
            (bool)
                Database consistency state
        """
        return self.conn.is_consistent(graph_uri)

    def explain_inference(self, content):
        """
        Return explanation for the given inference results

        Parameters
            content (Content)
                Data from which to provide explanations

        Returns
            (dict)
                Explanation results

        Example
            >> conn.explain_inference(File('inferences.ttl'))
        """
        with content.data() as data:
            return self.conn.explain_inference(data, content.content_type,
                                               content.content_encoding,
                                               self.transaction)

    def explain_inconsistency(self, graph_uri=None):
        """
        Explain why database or a specific named graph is inconsistent

        Parameters
            graph_uri (str)
                Named graph for which to explain inconsistency (optional)

        Returns
            (dict)
                Explanation results

        """
        return self.conn.explain_inconsistency(self.transaction,
                                               self.graph_uri)

    def _assert_not_in_transaction(self):
        if self.transaction:
            raise TransactionException('Already in a transaction')

    def _assert_in_transaction(self):
        if not self.transaction:
            raise TransactionException('Not in a transaction')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.conn.__exit__(*args)


class Docs(object):
    """
    BITES: Document Storage

    See Also
        https://www.stardog.com/docs/#_unstructured_data
    """

    def __init__(self, conn):
        self.docs = conn.conn.docs()

    def size(self):
        """
        Document store size

        Returns
            (int)
                Number of documents in the store
        """
        return self.docs.size()

    def add(self, name, content):
        """
        Add document to store

        Parameters
            name (str)
                Name of the document
            content (Content)
                Contents of the document

        Example
            >> docs.add('example', File('example.pdf'))
        """
        with content.data() as data:
            self.docs.add(name, data)

    def clear(self):
        """
        Remove all documents from the store
        """
        self.docs.clear()

    def get(self, name, stream=False, chunk_size=10240):
        """
        Get document from the store

        Parameters
            name (str)
                Name of the document
            stream (bool)
                If document should come in chunks or as a whole.
                Defaults to False
            chunk_size (int)
                Number of bytes to read per chunk when streaming.
                Defaults to 10240

        Returns
            (str)
                If stream=False
            (gen)
                If stream=True

        Example
            # no streaming
            >> contents = docs.get('example')

            # streaming
            >> with docs.get('example', stream=True) as stream:
                contents = ''.join(stream)
        """
        doc = self.docs.get(name, stream, chunk_size)
        return nextcontext(doc) if stream else next(doc)

    def delete(self, name):
        """
        Delete document from the store

        Parameters
            name (str)
                Name of the document
        """
        self.docs.delete(name)


class ICV(object):
    """
    Integrity Constraint Validation

    See Also
        https://www.stardog.com/docs/#_validating_constraints
    """

    def __init__(self, conn):
        self.conn = conn
        self.icv = conn.conn.icv()

    def add(self, content):
        """
        Add integrity constraints to database

        Parameters
            content (Content)
                Data to add

        Example
            >> icv.add(File('constraints.ttl'))
        """
        with content.data() as data:
            self.icv.add(data, content.content_type, content.content_encoding)

    def remove(self, content):
        """
        Remove integrity constraints from database

        Parameters
            content (Content)
                Data to remove

        Example
            >> icv.remove(File('constraints.ttl'))
        """
        with content.data() as data:
            self.icv.remove(data, content.content_type,
                            content.content_encoding)

    def clear(self):
        """
        Remove all integrity constraints from database
        """
        self.icv.clear()

    def is_valid(self, content, graph_uri=None):
        """
        Checks if given integrity constraints are valid

        Parameters
            content (Content)
                Data to check for validity
            graph_uri (str)
                Named graph from which to check validity from (optional)

        Returns
            (bool)
                Integrity constraint validity

        Example
            >> icv.is_valid(File('constraints.ttl'), graph_uri='urn:graph')
        """
        with content.data() as data:
            return self.icv.is_valid(data, content.content_type,
                                     content.content_encoding,
                                     self.conn.transaction, graph_uri)

    def explain_violations(self, content, graph_uri=None):
        """
        Explain violations for the given integrity constraints

        Parameters
            content (Content)
                Data to check for violations
            graph_uri (str)
                Named graph from which to check for validations (optional)

        Returns
            (dict)
                Integrity constraint violations

        Example
            >> icv.explain_violations(File('constraints.ttl'),
                                      graph_uri='urn:graph')
        """

        with content.data() as data:
            return self.icv.explain_violations(
                data, content.content_type, content.content_encoding,
                self.conn.transaction, graph_uri)

    def convert(self, content, graph_uri=None):
        """
        Convert given integrity constraints to a SPARQL query

        Parameters
            content (Content)
                Integrity constraints
            graph_uri (str)
                Named graph from which to apply constraints (optional)

        Returns
            (str)
                SPARQL query

        Example
            >> icv.convert(File('constraints.ttl'), graph_uri='urn:graph')
        """
        with content.data() as data:
            return self.icv.convert(data, content.content_type,
                                    content.content_encoding, graph_uri)


class VCS(object):
    """
    Versioning

    See Also
        https://www.stardog.com/docs/#_versioning
    """

    def __init__(self, conn):
        self.vcs = conn.conn.versioning()
        self.conn = conn

    def select(self, query, content_type=SPARQL_JSON, **kwargs):
        """
        Execute a SPARQL select query over versioning history

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)
            content_type (str)
                Content type for results.
                Defaults to 'application/sparql-results+json'

        Returns
            (dict)
                If content_type='application/sparql-results+json'
            (str)
                Other content types

        Example
            >> vcs.select('select distinct ?v {?v a vcs:Version}',
                          offset=100, limit=100)

            # bindings
            >> vcs.select(
                 'select distinct ?v {?v a ?o}',
                 bindings={'o': '<tag:stardog:api:versioning:Version>'})
        """
        return self.vcs.query(query, content_type, **kwargs)

    def graph(self, query, content_type=TURTLE, **kwargs):
        """
        Execute a SPARQL graph query over versioning history

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)
            content_type (str)
                Content type for results. Defaults to 'text/turtle'

        Returns
            (str)
                Results in format given by content_type

        Example
            >> vcs.graph('construct {?s ?p ?o} where {?s ?p ?o}',
                         offset=100, limit=100)
        """
        return self.vcs.query(query, content_type, **kwargs)

    def paths(self, query, content_type=SPARQL_JSON, **kwargs):
        """
        Execute a SPARQL paths query over versioning history

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)
            content_type (str)
                Content type for results.
                Defaults to 'application/sparql-results+json'

        Returns
            (dict)
                If content_type='application/sparql-results+json'
            (str)
                Other content types

        Example
            >> vcs.paths('paths start ?x = vcs:user:admin end'
                         ' ?y = <http://www.w3.org/ns/prov#Person> via ?p'))
        """
        return self.vcs.query(query, content_type, **kwargs)

    def ask(self, query, **kwargs):
        """
        Execute a SPARQL ask query over versioning history

        Parameters
            query (str)
                SPARQL query
            base_uri (str)
                Base URI for the parsing of the query (optional)
            limit (int)
                Maximum number of results to return (optional)
            offset (int)
                Offset into the result set (optional)
            timeout (int)
                Number of ms after which the query should timeout.
                0 or less implies no timeout (optional)
            reasoning (bool)
                Enable reasoning for the query (optional)
            bindings (dict)
                Map between query variables and their values (optional)

        Returns
            (bool)
                Result of ask query

        Example
            >> vcs.ask('ask {?v a vcs:Version}')
        """
        r = self.vcs.query(query, BOOLEAN, **kwargs)
        return bool(strtobool(r.decode()))

    def commit(self, message):
        """
        Commit current transaction into versioning

        Parameters
            message (str)
                Commit message

        Raises
            TransactionException
                If currently not in a transaction
        """
        self.conn._assert_in_transaction()
        self.vcs.commit(self.conn.transaction, message)

    def create_tag(self, revision, name):
        """
        Creates a new versioning tag

        Parameters
            revision (str)
                Revision ID
            name (str)
                Tag name

        Example
            >> vcs.create_tag('02cf7ed8e511bb4d62421565b42fffcaf00f5012',
                              'v1.1')
        """
        self.vcs.create_tag(revision, name)

    def delete_tag(self, name):
        """
        Deletes a versioning tag

        Parameters
            name (str)
                Tag name

        Example
            >> vcs.delete_tag('v1.1')
        """
        self.vcs.delete_tag(name)

    def revert(self, to_revision, from_revision, message):
        """
        Revert database to a specific revision

        Parameters
            to_revision (str)
                Begin revision ID
            from_revision (str)
                End revision ID
            message (str)
                Revert message

        Example
            >> vcs.revert('02cf7ed8e511bb4d62421565b42fffcaf00f5012',
                          '3c48bed3c1f9cfb056b4b0a755961a54d814f496',
                          'Log message')
        """
        self.vcs.revert(to_revision, from_revision, message)


class GraphQL(object):
    """
    GraphQL

    See Also:
        https://www.stardog.com/docs/#_graphql_queries
    """

    def __init__(self, conn):
        self.gql = conn.conn.graphql()
        self.conn = conn

    def query(self, query, variables=None):
        """
        Execute GraphQL query

        Parameters
            query (str)
                GraphQL query
            variables (dict)
                GraphQL variables (optional)
                Two special variables available:
                  @reasoning to enable reasoning,
                  @schema to define schemas

        Returns
            (dict)
                Query results

        Example
            # with schema and reasoning
            >> gql.query('{ Person {name} }',
                         variables={'@reasoning': True, '@schema': 'people'})

            # with named variables
            >> gql.query(
                 'query getPerson($id: Integer) { Person(id: $id) {name} }',
                 variables={'id': 1000})
        """
        return self.gql.query(query, variables)

    def schemas(self):
        """
        Retrieve all available schemas

        Returns
            (dict)
                All schemas
        """
        return self.gql.schemas()

    def clear_schemas(self):
        """
        Delete all schemas
        """
        self.gql.clear_schemas()

    def add_schema(self, name, content):
        """
        Add schema to database

        Parameters
            name (str)
                Name of the schema
            content (Content)
                Schema data

        Example
            >> gql.add_schema('people', content=File('people.graphql'))
        """
        with content.data() as data:
            self.gql.add_schema(name, data)

    def schema(self, name):
        """
        Get schema information

        Parameters
            name (str)
                Name of the schema

        Returns
            (dict)
                GraphQL schema
        """
        return self.gql.schema(name)

    def remove_schema(self, name):
        """
        Remove schema from database

        Parameters
            name (str)
                Name of the schema
        """
        return self.gql.remove_schema(name)


@contextmanager
def nextcontext(r):
    yield next(r)
