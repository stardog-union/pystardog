"""Connect to Stardog databases.
"""

import contextlib
import distutils.util

from . import content_types as content_types
from . import exceptions as exceptions
from .http import client


class Connection(object):
    """Database Connection.

    This is the entry point for all user-related operations on a
    Stardog database
    """

    def __init__(self, database, endpoint=None, username=None, password=None, auth=None):
        """Initializes a connection to a Stardog database.

        Args:
          database (str): Name of the database
          endpoint (str): Url of the server endpoint.
            Defaults to `http://localhost:5820`
          username (str, optional): Username to use in the connection
          password (str, optional): Password to use in the connection
          auth (requests.auth.AuthBase, optional): requests Authentication object.
            Defaults to `None`

        Examples:
          >>> conn = Connection('db', endpoint='http://localhost:9999',
                                username='admin', password='admin')
        """
        self.client = client.Client(endpoint, database, username, password, auth=auth)
        self.transaction = None

    def docs(self):
        """Makes a document storage object.

        Returns:
          Docs: A Docs object
        """
        return Docs(self.client)

    def icv(self):
        """Makes an integrity constraint validation object.

        Returns:
          ICV: An ICV object
        """
        return ICV(self)

    def graphql(self):
        """Makes a GraphQL object.

        Returns:
          GraphQL: A GraphQL object
        """
        return GraphQL(self)

    #TODO
    # def list(self):
    #     """
    #     List all open transactions on a database
    #     https://stardog-union.github.io/http-docs/#operation/listTransactions
    #     :return:
    #     """

    # TODO: Begin a transaction that takes a specific txid (the docs specify /begin and /begin/{txid}, I am not sure if this supports the latter)
    #   Maybe this already supports it, or we might need to create another method
    def begin(self, **kwargs):
        """Begins a transaction.

        Args:
          reasoning (bool, optional): Enable reasoning for all queries
            inside the transaction. If the transaction does not have reasoning
            enabled, queries within will not be able to use reasoning.

        Returns:
          str: Transaction ID

        Raises:
            stardog.exceptions.TransactionException
              If already in a transaction
        """
        self._assert_not_in_transaction()
        r = self.client.post('/transaction/begin', params=kwargs)
        self.transaction = r.text
        return self.transaction

    def rollback(self):
        """Rolls back the current transaction.

        Raises:
            stardog.exceptions.TransactionException
              If currently not in a transaction

        """
        self._assert_in_transaction()
        self.client.post('/transaction/rollback/{}'.format(self.transaction))
        self.transaction = None

    def commit(self):
        """Commits the current transaction.

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction
        """
        self._assert_in_transaction()
        self.client.post('/transaction/commit/{}'.format(self.transaction))
        self.transaction = None

    def add(self, content, graph_uri=None):
        """Adds data to the database.

        Args:
          content (Content): Data to add
          graph_uri (str, optional): Named graph into which to add the data

        Raises:
          stardog.exceptions.TransactionException
            If not currently in a transaction

        Examples:
          >>> conn.add(File('example.ttl'), graph_uri='urn:graph')
        """
        self._assert_in_transaction()

        with content.data() as data:
            self.client.post(
                '/{}/add'.format(self.transaction),
                params={'graph-uri': graph_uri},
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
                data=data)

    def remove(self, content, graph_uri=None):
        """Removes data from the database.

        Args:
          content (Content): Data to add
          graph_uri (str, optional): Named graph from which to remove the data

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction

        Examples:
          >>> conn.remove(File('example.ttl'), graph_uri='urn:graph')
        """

        self._assert_in_transaction()

        with content.data() as data:
            self.client.post(
                '/{}/remove'.format(self.transaction),
                params={'graph-uri': graph_uri},
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
                data=data)

    def clear(self, graph_uri=None):
        """Removes all data from the database or specific named graph.

        Args:
          graph_uri (str, optional): Named graph from which to remove data

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction

        Examples:
          clear a specific named graph

          >>> conn.clear('urn:graph')

          clear the whole database

          >>> conn.clear()
        """
        self._assert_in_transaction()
        self.client.post(
            '/{}/clear'.format(self.transaction),
            params={'graph-uri': graph_uri}
        )

    def size(self, exact=False):
        """Database size.

        Args:
          exact (bool, optional): Calculate the size exactly. Defaults to False

        Returns:
          int: The number of elements in database
        """
        r = self.client.get('/size', params={'exact': exact})
        return int(r.text)

    #TODO: This could be part of dbms-admin (hence moved into admin.py) or here.
    # def model(self):
    #     """
    #     Generate the reasoning model used by this database in various formats
    #     https://stardog-union.github.io/http-docs/#operation/generateModel
    #     :return:
    #     """

    def export(self,
               content_type=content_types.TURTLE,
               stream=False,
               chunk_size=10240,
               graph_uri=None):
        """Exports the contents of the database.

        Args:
          content_type (str): RDF content type. Defaults to 'text/turtle'
          stream (bool): Chunk results? Defaults to False
          chunk_size (int): Number of bytes to read per chunk when streaming.
            Defaults to 10240
          graph_uri (str, optional): Named graph to export

        Returns:
          str: If stream = False

        Returns:
          gen: If stream = True

        Examples:
          no streaming

          >>> contents = conn.export()

          streaming

          >>> with conn.export(stream=True) as stream:
                contents = ''.join(stream)
        """
        def _export():
            with self.client.get(
                    '/export', headers={'Accept': content_type},
                    params={'graph-uri': graph_uri},
                    stream=stream) as r:
                yield r.iter_content(
                    chunk_size=chunk_size) if stream else r.content
        db = _export()
        return _nextcontext(db) if stream else next(db)

    def explain(self, query, base_uri=None):
        """Explains the evaluation of a SPARQL query.

        Args:
          query (str): SPARQL query
          base_uri (str, optional): Base URI for the parsing of the query

        Returns:
         str: Query explanation
        """
        params = {'query': query, 'baseURI': base_uri}

        r = self.client.post(
            '/explain',
            data=params,
        )

        return r.text

    def __query(self,
                query,
                method,
                content_type=None,
                **kwargs):
        txId = self.transaction
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

        url = '/{}/{}'.format(txId, method) if txId else '/{}'.format(method)

        r = self.client.post(
            url,
            data=params,
            headers={'Accept': content_type},
        )

        return r.json(
        ) if content_type == content_types.SPARQL_JSON else r.content

    def select(self, query, content_type=content_types.SPARQL_JSON, **kwargs):
        """Executes a SPARQL select query.

        Args:
          query (str): SPARQL query
          base_uri (str, optional): Base URI for the parsing of the query
          limit (int, optional): Maximum number of results to return
          offset (int, optional): Offset into the result set
          timeout (int, optional): Number of ms after which the query should
            timeout. 0 or less implies no timeout
          reasoning (bool, optional): Enable reasoning for the query
          bindings (dict, optional): Map between query variables and their
            values
          content_type (str, optional): Content type for results.
            Defaults to 'application/sparql-results+json'

        Returns:
          dict: If content_type='application/sparql-results+json'

        Returns:
          str: Other content types

        Examples:
          >>> conn.select('select * {?s ?p ?o}',
                          offset=100, limit=100, reasoning=True)

          bindings

          >>> conn.select('select * {?s ?p ?o}', bindings={'o': '<urn:a>'})
        """
        return self.__query(query, 'query', content_type=content_type, **kwargs)

    def graph(self, query, content_type=content_types.TURTLE, **kwargs):
        """Executes a SPARQL graph query.

        Args:
          query (str): SPARQL query
          base_uri (str, optional): Base URI for the parsing of the query
          limit (int, optional): Maximum number of results to return
          offset (int, optional): Offset into the result set
          timeout (int, optional): Number of ms after which the query should
            timeout. 0 or less implies no timeout
          reasoning (bool, optional): Enable reasoning for the query
          bindings (dict, optional): Map between query variables and their
            values
          content_type (str): Content type for results.
            Defaults to 'text/turtle'

        Returns:
          str: Results in format given by content_type

        Examples:
          >>> conn.graph('construct {?s ?p ?o} where {?s ?p ?o}',
                         offset=100, limit=100, reasoning=True)

          bindings

          >>> conn.graph('construct {?s ?p ?o} where {?s ?p ?o}',
                         bindings={'o': '<urn:a>'})
        """
        return self.__query(query, 'query', content_type, **kwargs)

    def paths(self, query, content_type=content_types.SPARQL_JSON, **kwargs):
        """Executes a SPARQL paths query.

        Args:
          query (str): SPARQL query
          base_uri (str, optional): Base URI for the parsing of the query
          limit (int, optional): Maximum number of results to return
          offset (int, optional): Offset into the result set
          timeout (int, optional): Number of ms after which the query should
            timeout. 0 or less implies no timeout
          reasoning (bool, optional): Enable reasoning for the query
          bindings (dict, optional): Map between query variables and their
            values
          content_type (str): Content type for results.
              Defaults to 'application/sparql-results+json'

        Returns:
          dict: if content_type='application/sparql-results+json'.

        Returns:
          str: other content types.

        Examples:
          >>> conn.paths('paths start ?x = :subj end ?y = :obj via ?p',
                         reasoning=True)
        """
        return self.__query(query, 'query', content_type, **kwargs)

    def ask(self, query, **kwargs):
        """Executes a SPARQL ask query.

        Args:
          query (str): SPARQL query
          base_uri (str, optional): Base URI for the parsing of the query
          limit (int, optional): Maximum number of results to return
          offset (int, optional): Offset into the result set
          timeout (int, optional): Number of ms after which the query should
            timeout. 0 or less implies no timeout
          reasoning (bool, optional): Enable reasoning for the query
          bindings (dict, optional): Map between query variables and their
            values

        Returns:
          bool: Result of ask query

        Examples:
          >>> conn.ask('ask {:subj :pred :obj}', reasoning=True)
        """
        r = self.__query(query, 'query', content_types.BOOLEAN, **kwargs)
        return bool(distutils.util.strtobool(r.decode()))

    def update(self, query, **kwargs):
        """Executes a SPARQL update query.

        Args:
          query (str): SPARQL query
          base_uri (str, optional): Base URI for the parsing of the query
          limit (int, optional): Maximum number of results to return
          offset (int, optional): Offset into the result set
          timeout (int, optional): Number of ms after which the query should
            timeout. 0 or less implies no timeout
          reasoning (bool, optional): Enable reasoning for the query
          bindings (dict, optional): Map between query variables and their
            values

        Examples:
          >>> conn.update('delete where {?s ?p ?o}')
        """
        self.__query(query, 'update', None, **kwargs)

    def is_consistent(self, graph_uri=None):
        """Checks if the database or named graph is consistent wrt its schema.

        Args:
          graph_uri (str, optional): Named graph from which to check
            consistency

        Returns:
          bool: Database consistency state
        """
        r = self.client.get(
            '/reasoning/consistency',
            params={'graph-uri': graph_uri},
        )

        return bool(distutils.util.strtobool(r.text))

    def explain_inference(self, content):
        """Explains the given inference results.

        Args:
          content (Content): Data from which to provide explanations

        Returns:
          dict: Explanation results

        Examples:
          >>> conn.explain_inference(File('inferences.ttl'))
        """
        txId = self.transaction

        with content.data() as data:
            url = '/reasoning/{}/explain'.format(txId) if txId else '/reasoning/explain'

            r = self.client.post(
                url,
                data=data,
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
            )

            return r.json()['proofs']

    def explain_inconsistency(self, graph_uri=None):
        """Explains why the database or a named graph is inconsistent.

        Args:
          graph_uri (str, optional): Named graph for which to explain
            inconsistency

        Returns:
          dict: Explanation results
        """
        txId = self.transaction
        url = '/reasoning/{}/explain/inconsistency'.format(
            txId) if txId else '/reasoning/explain/inconsistency'

        r = self.client.get(
            url,
            params={'graph-uri': graph_uri},
        )

        return r.json()['proofs']

    def _assert_not_in_transaction(self):
        if self.transaction:
            raise exceptions.TransactionException('Already in a transaction')

    def _assert_in_transaction(self):
        if not self.transaction:
            raise exceptions.TransactionException('Not in a transaction')

    def close(self):
        """Close the underlying HTTP connection.
        """
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Docs(object):
    """BITES: Document Storage.

    See Also:
      https://www.stardog.com/docs/#_unstructured_data
    """

    def __init__(self, client):
        """Initializes a Docs.

        Use :meth:`stardog.connection.Connection.docs`
        instead of constructing manually.
        """
        self.client = client

    def size(self):
        """Calculates document store size.

        Returns:
          int: Number of documents in the store
        """
        r = self.client.get('/docs/size')
        return int(r.text)

    def add(self, name, content):
        """Adds a document to the store.

        Args:
          name (str): Name of the document
          content (Content): Contents of the document

        Examples:
          >>> docs.add('example', File('example.pdf'))
        """
        with content.data() as data:
            self.client.post('/docs', files={'upload': (name, data)})

    def clear(self):
        """Removes all documents from the store.
        """
        self.client.delete('/docs')

    #TODO
    # def reindex(self):
    #     """
    #     Reindex document store
    #     https://stardog-union.github.io/http-docs/#operation/reindex
    #     :return:
    #     """

    def get(self, name, stream=False, chunk_size=10240):
        """Gets a document from the store.

        Args:
          name (str): Name of the document
          stream (bool): If document should come in chunks or as a whole.
            Defaults to False
          chunk_size (int): Number of bytes to read per chunk when streaming.
            Defaults to 10240

        Returns:
          str: If stream=False

        Returns:
          gen: If stream=True

        Examples:
          no streaming

          >>> contents = docs.get('example')

          streaming

          >>> with docs.get('example', stream=True) as stream:
                            contents = ''.join(stream)
        """
        def _get():
            with self.client.get('/docs/{}'.format(name), stream=stream) as r:
                yield r.iter_content(
                    chunk_size=chunk_size) if stream else r.content

        doc = _get()
        return _nextcontext(doc) if stream else next(doc)

    def delete(self, name):
        """Deletes a document from the store.

        Args:
          name (str): Name of the document
        """
        self.client.delete('/docs/{}'.format(name))


class ICV(object):
    """Integrity Constraint Validation.

    See Also:
      https://www.stardog.com/docs/#_validating_constraints
    """

    def __init__(self, conn):
        """Initializes an ICV.

        Use :meth:`stardog.connection.Connection.icv`
        instead of constructing manually.
        """
        self.conn = conn
        self.client = conn.client

    def add(self, content):
        """Adds integrity constraints to the database.

        Args:
          content (Content): Data to add

        Examples:
          >>> icv.add(File('constraints.ttl'))
        """
        with content.data() as data:
            self.client.post(
                '/icv/add',
                data=data,
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
            )

    def remove(self, content):
        """Removes integrity constraints from the database.

        Args:
          content (Content): Data to remove

        Examples:
          >>> icv.remove(File('constraints.ttl'))
        """
        with content.data() as data:
            self.client.post(
                '/icv/remove',
                data=data,
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
            )

    def clear(self):
        """Removes all integrity constraints from the database.
        """
        self.client.post('/icv/clear')

    def list(self):
        """List all integrity constraints from the database.
        """
        r = self.client.get('/icv')
        return r.text

    def is_valid(self, content, graph_uri=None):
        """Checks if given integrity constraints are valid.

        Args:
          content (Content): Data to check for validity
          graph_uri (str, optional): Named graph to check for validity

        Returns:
          bool: Integrity constraint validity

        Examples:
          >>> icv.is_valid(File('constraints.ttl'), graph_uri='urn:graph')
        """
        transaction = self.conn.transaction
        url = 'icv/{}/validate'.format(
            transaction) if transaction else '/icv/validate'
        with content.data() as data:
            r = self.client.post(
                url,
                data=data,
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
                params={'graph-uri': graph_uri},
            )

            return bool(distutils.util.strtobool(r.text))

    def explain_violations(self, content, graph_uri=None):
        """Explains violations of the given integrity constraints.

        Args:
          content (Content): Data to check for violations
          graph_uri (str, optional): Named graph from which to check for
            validations

        Returns:
          dict: Integrity constraint violations

        Examples:
          >>> icv.explain_violations(File('constraints.ttl'),
                                     graph_uri='urn:graph')
        """
        transaction = self.conn.transaction
        url = '/icv/{}/violations'.format(
            transaction) if transaction else '/icv/violations'

        with content.data() as data:
            r = self.client.post(
                url,
                data=data,
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
                params={'graph-uri': graph_uri},
            )

            return self.client._multipart(r)

    def convert(self, content, graph_uri=None):
        """Converts given integrity constraints to a SPARQL query.

        Args:
          content (Content): Integrity constraints
          graph_uri (str, optional): Named graph from which to apply
            constraints

        Returns:
          str: SPARQL query

        Examples:
          >>> icv.convert(File('constraints.ttl'), graph_uri='urn:graph')
        """
        with content.data() as data:
            r = self.client.post(
                '/icv/convert',
                data=data,
                headers={
                    'Content-Type': content.content_type,
                    'Content-Encoding': content.content_encoding
                },
                params={'graph-uri': graph_uri},
            )

            return r.text


class GraphQL(object):
    """GraphQL

    See Also:
        https://www.stardog.com/docs/#_graphql_queries

    """

    def __init__(self, conn):
        """Initializes a GraphQL.

        Use :meth:`stardog.connection.Connection.graphql`
        instead of constructing manually.
        """
        self.conn = conn
        self.client = conn.client

    def query(self, query, variables=None):
        """Executes a GraphQL query.

        Args:
          query (str): GraphQL query
          variables (dict, optional): GraphQL variables.
            Keys: '@reasoning' (bool) to enable reasoning,
            '@schema' (str) to define schemas

        Returns:
          dict: Query results

        Examples:
          with schema and reasoning

          >>> gql.query('{ Person {name} }',
                        variables={'@reasoning': True, '@schema': 'people'})

          with named variables

          >>> gql.query(
                'query getPerson($id: Integer) { Person(id: $id) {name} }',
                variables={'id': 1000})

        """
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
        """Retrieves all available schemas.

        Returns:
          dict: All schemas
        """
        r = self.client.get('/graphql/schemas')
        return r.json()['schemas']

    def clear_schemas(self):
        """Deletes all schemas.
        """
        self.client.delete('/graphql/schemas')

    def add_schema(self, name, content):
        """Adds a schema to the database.

        Args:
          name (str): Name of the schema
          content (Content): Schema data

        Examples:
          >>> gql.add_schema('people', content=File('people.graphql'))
        """
        with content.data() as data:
            self.client.put('/graphql/schemas/{}'.format(name), data=data)

    def schema(self, name):
        """Gets schema information.

        Args:
          name (str): Name of the schema

        Returns:
          dict: GraphQL schema
        """
        r = self.client.get('/graphql/schemas/{}'.format(name))
        return r.text

    def remove_schema(self, name):
        """Removes a schema from the database.

        Args:
          name (str): Name of the schema
        """
        self.client.delete('/graphql/schemas/{}'.format(name))


@contextlib.contextmanager
def _nextcontext(r):
    yield next(r)
