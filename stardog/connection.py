"""Connect to Stardog databases.
"""

import contextlib
from typing import Iterator, Optional, TypedDict, Union, Dict, List

from requests.auth import AuthBase
from requests.sessions import Session

from stardog.content import Content

from . import content_types as content_types
from . import exceptions as exceptions
from .http import client
from .utils import strtobool
import urllib


class CommitResult(TypedDict):
    """The result of committing a transaction.

    Represents the outcome of a transaction commit operation, including the counts
    of added and removed triples.
    """

    added: int
    """ The amount of triples added in the transaction """

    removed: int
    """ The amount of triples removed in the transaction """


class Connection:
    """Database Connection.

    This is the entry point for all user-related operations on a
    Stardog database
    """

    def __init__(
        self,
        database: str,
        endpoint: Optional[str] = client.Client.DEFAULT_ENDPOINT,
        username: Optional[str] = client.Client.DEFAULT_USERNAME,
        password: Optional[str] = client.Client.DEFAULT_PASSWORD,
        auth: Optional[AuthBase] = None,
        session: Optional[Session] = None,
        run_as: Optional[str] = None,
    ):
        """Initializes a connection to a Stardog database.

        :param database: Name of the database
        :param endpoint: URL of the Stardog server endpoint.
        :param username: Username to use in the connection.
        :param password: Password to use in the connection.
        :param auth: :class:`requests.auth.AuthBase` object. Used as an alternative authentication scheme. If not provided, HTTP Basic auth will be attempted with the ``username`` and ``password``.
        :param session: :class:`requests.session.Session` object
        :param run_as: The user to impersonate.

        Examples:
          >>> conn = Connection('db', endpoint='http://localhost:9999',
                                username='admin', password='admin')
        """
        self.client = client.Client(
            endpoint,
            database,
            username,
            password,
            auth=auth,
            session=session,
            run_as=run_as,
        )
        self.transaction = None

    def docs(self) -> "Docs":
        """Makes a document storage object."""
        return Docs(self.client)

    def icv(self) -> "ICV":
        """Makes an integrity constraint validation (ICV) object."""
        return ICV(self)

    def graphql(self) -> "GraphQL":
        """Makes a GraphQL object."""
        return GraphQL(self)

    # TODO
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
        r = self.client.post("/transaction/begin", params=kwargs)
        self.transaction = r.text
        return self.transaction

    def rollback(self):
        """Rolls back the current transaction.

        Raises:
            stardog.exceptions.TransactionException
              If currently not in a transaction

        """
        self._assert_in_transaction()
        self.client.post("/transaction/rollback/{}".format(self.transaction))
        self.transaction = None

    def commit(self) -> CommitResult:
        """Commits the current transaction.

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction
        """
        self._assert_in_transaction()
        resp = self.client.post("/transaction/commit/{}".format(self.transaction))
        self.transaction = None

        return resp.json()

    def add(
        self,
        content: Union[Content, str],
        graph_uri: Optional[str] = None,
        server_side: bool = False,
    ) -> None:
        """Adds data to the database.

        :param content: Data to add to a graph.
        :param graph_uri: Named graph into which to add the data. If no named graph is provided, the data will be loaded into the default graph.
        :param server_side: Whether the file to load is located on the same file system as the Stardog server.

        Raises:
          stardog.exceptions.TransactionException
            If currently not in a transaction

        Examples:

            Loads ``example.ttl`` from the current directory

            >>> conn.add(File('example.ttl'), graph_uri='urn:graph')

            Loads ``/tmp/example.ttl`` which exists on the same file system as the Stardog server,
            and loads it in the default graph.

            >>> conn.add(File('/tmp/example.ttl'), server_side=True)

        """
        self._assert_in_transaction()

        args = {"params": {"graph-uri": graph_uri}}

        if server_side:
            args["headers"] = {
                "Content-Type": "application/json",
            }
            args["json"] = {
                "filename": content.fname,
            }
            self.client.post("/{}/add".format(self.transaction), **args)
        else:
            with content.data() as data:
                args["headers"] = {
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                }
                args["data"] = data
                self.client.post("/{}/add".format(self.transaction), **args)

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
                "/{}/remove".format(self.transaction),
                params={"graph-uri": graph_uri},
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
                data=data,
            )

    def clear(self, graph_uri: Optional[str] = None) -> None:
        """Removes all data from the database or specific named graph.

        :param graph_uri: Named graph from which to remove data.

        .. warning::
            If no ``graph_uri`` is specified, the entire database will be cleared.

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
            "/{}/clear".format(self.transaction), params={"graph-uri": graph_uri}
        )

    def size(self, exact: bool = False) -> int:
        """Calculate the size of the database.

        :param exact: calculate the size of the database exactly. If ``False`` (default), the size will be an estimate; this should take less time to calculate especially if the database is large.
        :return: the number of triples in the database
        """
        r = self.client.get("/size", params={"exact": exact})
        return int(r.text)

    # TODO: This could be part of dbms-admin (hence moved into admin.py) or here.
    # def model(self):
    #     """
    #     Generate the reasoning model used by this database in various formats
    #     https://stardog-union.github.io/http-docs/#operation/generateModel
    #     :return:
    #     """

    def export(
        self,
        content_type: str = content_types.TURTLE,
        stream: bool = False,
        chunk_size: int = 10240,
        graph_uri: Optional[str] = None,
    ) -> Union[str, Iterator[bytes]]:
        """Exports the contents of the database.

        :param content_type: RDF content type.
        :param stream: Stream and chunk results?. See the note below for additional information.
        :param chunk_size: Number of bytes to read per chunk when streaming.
        :param graph_uri: URI of the named graph to export

        .. note::
            If ``stream=False`` (default), the contents of the database or named graph will be returned as a ``str``. If ``stream=True``, an iterable that yields chunks of content as ``bytes`` will be returned.

        Examples:
          No streaming

          >>> contents = conn.export()

          Streaming

          >>> with conn.export(stream=True) as stream:
                contents = ''.join(stream)
        """

        def _export():
            with self.client.get(
                "/export",
                headers={"Accept": content_type},
                params={"graph-uri": graph_uri},
                stream=stream,
            ) as r:
                yield r.iter_content(chunk_size=chunk_size) if stream else r.content

        db = _export()
        return _nextcontext(db) if stream else next(db)

    def explain(self, query: str, base_uri: Optional[str] = None) -> str:
        """Explains the evaluation of a SPARQL query.

        :param query: the SPARQL query to explain
        :param base_uri: base URI for the parsing of the ``query``

        :return: Query explanation
        """
        params = {"query": query, "baseURI": base_uri}

        r = self.client.post(
            "/explain",
            data=params,
        )

        return r.text

    def __query(
        self, query: str, method: str, content_type: Optional[str] = None, **kwargs
    ) -> Union[Dict, bytes]:
        txId = self.transaction
        params = {
            "query": query,
            "baseURI": kwargs.get("base_uri"),
            "limit": kwargs.get("limit"),
            "offset": kwargs.get("offset"),
            "timeout": kwargs.get("timeout"),
            "reasoning": kwargs.get("reasoning"),
            "prettify": kwargs.get("prettify"),
            "default-graph-uri": kwargs.get("default_graph_uri"),
            "named-graph-uri": kwargs.get("named_graph_uri"),
            "using-graph-uri": kwargs.get("using_graph_uri"),
            "using-named-graph-uri": kwargs.get("using_named_graph_uri"),
            "remove-graph-uri": kwargs.get("remove_graph_uri"),
            "insert-graph-uri": kwargs.get("insert_graph_uri"),
            "schema": kwargs.get("schema"),
        }

        # query bindings
        bindings = kwargs.get("bindings", {})

        # many of the query methods (select, ask, etc) set bindings to a
        # default of None
        if bindings is None:
            bindings = {}
        for k, v in bindings.items():
            params["${}".format(k)] = v

        url = "/{}/{}".format(txId, method) if txId else "/{}".format(method)

        r = self.client.post(
            url,
            data=params,
            headers={"Accept": content_type},
        )

        return r.json() if content_type == content_types.SPARQL_JSON else r.content

    def select(
        self,
        query: str,
        base_uri: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        timeout: Optional[int] = None,
        reasoning: Optional[bool] = None,
        bindings: Optional[Dict[str, str]] = None,
        content_type: str = content_types.SPARQL_JSON,
        default_graph_uri: Optional[List[str]] = None,
        named_graph_uri: Optional[List[str]] = None,
        **kwargs,
    ) -> Union[bytes, Dict]:
        """Executes a SPARQL select query.

        :param query: SPARQL query
        :param base_uri: Base URI for the parsing of the query
        :param limit: Maximum number of results to return
        :param offset: Offset into the result set
        :param timeout: Number of ms after which the query should
          timeout. ``0`` or less implies no timeout
        :param reasoning: Enable reasoning for the query
        :param bindings: Map between query variables and their values
        :param content_type: Content type for results.
        :param default_graph_uri: URI(s) to be used as the default graph (equivalent to ``FROM``)
        :param named_graph_uri: URI(s) to be used as named graphs (equivalent to ``FROM NAMED``)

        :return: If ``content_type='application/sparql-results+json'``, results will be returned as a Dict, else results will be returned as bytes.

        **Examples:**

        .. code-block:: python
            :caption: Simple ``SELECT`` query utilizing ``limit`` and ``offset`` to restrict the result set with ``reasoning`` enabled.

            results = conn.select('select * {?s ?p ?o}',
                        offset=100,
                        limit=100,
                        reasoning=True
                      )


        .. code-block:: python
            :caption: Query utilizing ``bindings`` to bind the query variable ``o`` to a value of ``<urn:a>``.

            results = conn.select('select * {?s ?p ?o}', bindings={'o': '<urn:a>'})
        """
        return self.__query(
            query=query,
            method="query",
            content_type=content_type,
            base_uri=base_uri,
            limit=limit,
            offset=offset,
            timeout=timeout,
            reasoning=reasoning,
            bindings=bindings,
            default_graph_uri=default_graph_uri,
            named_graph_uri=named_graph_uri,
            **kwargs,
        )

    def graph(
        self,
        query: str,
        base_uri: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        timeout: Optional[int] = None,
        reasoning: Optional[bool] = None,
        bindings: Optional[Dict[str, str]] = None,
        content_type=content_types.TURTLE,
        default_graph_uri: Optional[List[str]] = None,
        named_graph_uri: Optional[List[str]] = None,
        **kwargs,
    ) -> bytes:
        """Executes a SPARQL graph (``CONSTRUCT``) query.

        :param query: SPARQL query
        :param base_uri: Base URI for the parsing of the query
        :param limit: Maximum number of results to return
        :param offset: Offset into the result set
        :param timeout: Number of ms after which the query should
          timeout. ``0`` or less implies no timeout
        :param reasoning: Enable reasoning for the query
        :param bindings: Map between query variables and their values
        :param content_type: Content type for results.
        :param default_graph_uri: URI(s) to be used as the default graph (equivalent to ``FROM``)
        :param named_graph_uri: URI(s) to be used as named graphs (equivalent to ``FROM NAMED``)

        :return: the query results

        **Examples:**

        .. code-block:: python
            :caption: Simple ``CONSTRUCT`` (graph) query utilizing ``limit`` and ``offset`` to restrict the result set with ``reasoning`` enabled.

            results = conn.graph('select * {?s ?p ?o}',
                        offset=100,
                        limit=100,
                        reasoning=True
                      )


        .. code-block:: python
            :caption: ``CONSTRUCT`` (graph) query utilizing ``bindings`` to bind the query variable ``o`` to a value of ``<urn:a>``.

            results = conn.graph('select * {?s ?p ?o}', bindings={'o': '<urn:a>'})
        """
        return self.__query(
            query=query,
            method="query",
            content_type=content_type,
            base_uri=base_uri,
            limit=limit,
            offset=offset,
            timeout=timeout,
            reasoning=reasoning,
            bindings=bindings,
            default_graph_uri=default_graph_uri,
            named_graph_uri=named_graph_uri,
            **kwargs,
        )

    def paths(
        self,
        query: str,
        base_uri: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        timeout: Optional[int] = None,
        reasoning: Optional[bool] = None,
        bindings: Optional[Dict[str, str]] = None,
        content_type=content_types.SPARQL_JSON,
        default_graph_uri: Optional[List[str]] = None,
        named_graph_uri: Optional[List[str]] = None,
        **kwargs,
    ) -> Union[Dict, bytes]:
        """Executes a SPARQL paths query.

        :param query: SPARQL query
        :param base_uri: Base URI for the parsing of the query
        :param limit: Maximum number of results to return
        :param offset: Offset into the result set
        :param timeout: Number of ms after which the query should
          timeout. ``0`` or less implies no timeout
        :param reasoning: Enable reasoning for the query
        :param bindings: Map between query variables and their values
        :param content_type: Content type for results.
        :param default_graph_uri: URI(s) to be used as the default graph (equivalent to ``FROM``)
        :param named_graph_uri: URI(s) to be used as named graphs (equivalent to ``FROM NAMED``)

        :return: If ``content_type='application/sparql-results+json'``, results will be returned as a Dict
            , else results will be returned as bytes.


        **Examples:**

        .. code-block:: python
            :caption: Simple ``PATHS`` query with ``reasoning`` enabled.

            results = conn.paths('paths start ?x = :subj end ?y = :obj via ?p', reasoning=True)

        See Also:
            `Stardog Docs - PATH Queries <https://docs.stardog.com/query-stardog/path-queries>`_

        """
        return self.__query(
            query=query,
            method="query",
            content_type=content_type,
            base_uri=base_uri,
            limit=limit,
            offset=offset,
            timeout=timeout,
            reasoning=reasoning,
            bindings=bindings,
            default_graph_uri=default_graph_uri,
            named_graph_uri=named_graph_uri,
            **kwargs,
        )

    def ask(
        self,
        query: str,
        base_uri: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        timeout: Optional[int] = None,
        reasoning: Optional[bool] = None,
        bindings: Optional[Dict[str, str]] = None,
        default_graph_uri: Optional[List[str]] = None,
        named_graph_uri: Optional[List[str]] = None,
        **kwargs,
    ) -> bool:
        """Executes a SPARQL ``ASK`` query.

        :param query: SPARQL query
        :param base_uri: Base URI for the parsing of the query
        :param limit: Maximum number of results to return
        :param offset: Offset into the result set
        :param timeout: Number of ms after which the query should
          timeout. ``0`` or less implies no timeout
        :param reasoning: Enable reasoning for the query
        :param bindings: Map between query variables and their values
        :param default_graph_uri: URI(s) to be used as the default graph (equivalent to ``FROM``)
        :param named_graph_uri: URI(s) to be used as named graphs (equivalent to ``FROM NAMED``)

        :return: whether the query pattern has a solution or not

        **Examples:**

        .. code-block:: python
            :caption: ``ASK`` query with ``reasoning`` enabled.

            pattern_exists = conn.ask('ask {:subj :pred :obj}', reasoning=True)

        See Also:
            `SPARQL Spec - ASK Queries <https://www.w3.org/TR/sparql11-query/#ask>`_

        """

        r = self.__query(
            query=query,
            method="query",
            content_type=content_types.BOOLEAN,
            base_uri=base_uri,
            limit=limit,
            offset=offset,
            timeout=timeout,
            reasoning=reasoning,
            bindings=bindings,
            default_graph_uri=default_graph_uri,
            named_graph_uri=named_graph_uri,
            **kwargs,
        )
        return strtobool(r.decode())

    def update(
        self,
        query: str,
        base_uri: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        timeout: Optional[int] = None,
        reasoning: Optional[bool] = None,
        bindings: Optional[Dict[str, str]] = None,
        using_graph_uri: Optional[List[str]] = None,
        using_named_graph_uri: Optional[List[str]] = None,
        remove_graph_uri: Optional[str] = None,
        insert_graph_uri: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Executes a SPARQL update query.

        :param query: SPARQL query
        :param base_uri: Base URI for the parsing of the query
        :param limit: Maximum number of results to return
        :param offset: Offset into the result set
        :param timeout: Number of ms after which the query should
          timeout. ``0`` or less implies no timeout
        :param reasoning: Enable reasoning for the query
        :param bindings: Map between query variables and their values
        :param using_graph_uri: URI(s) to be used as the default graph (equivalent to ``USING``)
        :param using_named_graph_uri: URI(s) to be used as named graphs (equivalent to ``USING NAMED``)
        :param remove_graph_uri: URI of the graph to be removed from
        :param insert_graph_uri: URI of the graph to be inserted into

        Examples:
          >>> conn.update('delete where {?s ?p ?o}')
        """

        self.__query(
            query=query,
            method="update",
            content_type=None,
            base_uri=base_uri,
            limit=limit,
            offset=offset,
            timeout=timeout,
            reasoning=reasoning,
            bindings=bindings,
            using_graph_uri=using_graph_uri,
            using_named_graph_uri=using_named_graph_uri,
            remove_graph_uri=remove_graph_uri,
            insert_graph_uri=insert_graph_uri,
            **kwargs,
        )

    def is_consistent(self, graph_uri: Optional[str] = None) -> bool:
        """Checks if the database or named graph is consistent with respect to its schema.

        :param graph_uri: the URI of the graph to check
        :return: database consistency state
        """
        r = self.client.get(
            "/reasoning/consistency",
            params={"graph-uri": graph_uri},
        )

        return strtobool(r.text)

    def explain_inference(self, content: Content) -> dict:
        """Explains the given inference results.

        :param content: data from which to provide explanations
        :return: explanation results

        Examples:
          >>> conn.explain_inference(File('inferences.ttl'))
        """
        txId = self.transaction

        with content.data() as data:
            url = "/reasoning/{}/explain".format(txId) if txId else "/reasoning/explain"

            r = self.client.post(
                url,
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
            )

            return r.json()["proofs"]

    def explain_inconsistency(self, graph_uri: Optional[str] = None) -> dict:
        """Explains why the database or a named graph is inconsistent.

        :param graph_uri: the URI of the named graph for which to explain inconsistency
        :return: explanation results
        """
        txId = self.transaction
        url = (
            "/reasoning/{}/explain/inconsistency".format(txId)
            if txId
            else "/reasoning/explain/inconsistency"
        )

        r = self.client.get(
            url,
            params={"graph-uri": graph_uri},
        )

        return r.json()["proofs"]

    def _assert_not_in_transaction(self):
        if self.transaction:
            raise exceptions.TransactionException("Already in a transaction")

    def _assert_in_transaction(self):
        if not self.transaction:
            raise exceptions.TransactionException("Not in a transaction")

    def close(self) -> None:
        """Close the underlying HTTP connection."""
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Docs:
    """BITES: Document Storage.

    See Also:
        `Stardog Docs - BITES <https://docs.stardog.com/unstructured-content/>`_
    """

    def __init__(self, client: client.Client):
        """Initializes a Docs.

        Use :meth:`stardog.connection.Connection.docs`
        instead of constructing manually.
        """
        self.client = client

    def size(self) -> int:
        """Calculates document store size.

        :return: Number of documents in the store
        """
        r = self.client.get("/docs/size")
        return int(r.text)

    def add(self, name: str, content: Content) -> None:
        """Adds a document to the store.

        :param name: Name of the document
        :param content: Contents of the document

        Examples:
          >>> docs.add('example', File('example.pdf'))
        """
        with content.data() as data:
            self.client.post("/docs", files={"upload": (name, data)})

    def clear(self) -> None:
        """Removes all documents from the store."""
        self.client.delete("/docs")

    # TODO
    # def reindex(self):
    #     """
    #     Reindex document store
    #     https://stardog-union.github.io/http-docs/#operation/reindex
    #     :return:
    #     """

    def get(
        self, name: str, stream: bool = False, chunk_size: int = 10240
    ) -> Union[str, Iterator[bytes]]:
        """Gets a document from the store.

        :param name: Name of the document
        :param stream: If document should be streamed back as chunks of bytes or as one string .
        :param chunk_size: Number of bytes to read per chunk when streaming.

        .. note::
            If ``stream=False``, the contents of the document will be returned as a ``str``. If ``stream=True``, an iterable that yields chunks of content as ``bytes`` will be returned.

        Examples:
          No streaming

          >>> contents = docs.get('example')

          Streaming

          >>> with docs.get('example', stream=True) as stream:
                            contents = ''.join(stream)
        """

        def _get():
            with self.client.get("/docs/{}".format(name), stream=stream) as r:
                yield r.iter_content(chunk_size=chunk_size) if stream else r.content

        doc = _get()
        return _nextcontext(doc) if stream else next(doc)

    def delete(self, name: str) -> None:
        """Deletes a document from the store.

        :param name: Name of the document to delete
        """
        self.client.delete("/docs/{}".format(name))


class ICV:
    """Integrity Constraint Validation.

    See Also:
      `Stardog Docs - Data Quality Constraints <https://docs.stardog.com/data-quality-constraints>`_
    """

    def __init__(self, conn: Connection):
        """Initializes an ICV.

        Use :meth:`stardog.connection.Connection.icv`
        instead of constructing manually.
        """
        self.conn = conn
        self.client = conn.client

    def add(self, content: Content) -> None:
        """
        Adds integrity constraints to the database.

        :param content: Data to add

        .. warning::
            Deprecated: :meth:`stardog.connection.Connection.add` should be preferred. :meth:`stardog.connection.ICV.add` will be removed in the next major version.

        Examples:
          >>> icv.add(File('constraints.ttl'))
        """
        with content.data() as data:
            self.client.post(
                "/icv/add",
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
            )

    def remove(self, content: Content) -> None:
        """
        Removes integrity constraints from the database.

        :param content: Data to remove

        .. warning::
            Deprecated: :meth:`stardog.connection.Connection.remove` should be preferred. :meth:`stardog.connection.ICV.remove` will be removed in the next major version.


        Examples:
          >>> icv.remove(File('constraints.ttl'))
        """
        with content.data() as data:
            self.client.post(
                "/icv/remove",
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
            )

    def clear(self) -> None:
        """Removes all integrity constraints from the database."""
        self.client.post("/icv/clear")

    def list(self) -> str:
        """List all integrity constraints from the database."""
        r = self.client.get("/icv")
        return r.text

    def is_valid(self, content: Content, graph_uri: Optional[str] = None) -> bool:
        """Checks if given integrity constraints are valid.

        :param content: Data to check validity (with respect to constraints) against
        :param graph_uri: URI of the named graph to check for validity
        :return: whether the data is valid with respect to the constraints

        Examples:
          >>> icv.is_valid(File('constraints.ttl'), graph_uri='urn:graph')
        """
        transaction = self.conn.transaction
        url = "icv/{}/validate".format(transaction) if transaction else "/icv/validate"
        with content.data() as data:
            r = self.client.post(
                url,
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
                params={"graph-uri": graph_uri},
            )

            return strtobool(r.text)

    def explain_violations(
        self, content: Content, graph_uri: Optional[str] = None
    ) -> Dict:
        """
        Explains violations of the given integrity constraints.

        :param content: Data containing constraints
        :graph_uri: Named graph from which to check for violations
        :return: the violations

        .. warning::
            Deprecated: :meth:`stardog.connection.ICV.report` should be preferred. :meth:`stardog.connection.ICV.explain_violations` will be removed in the next major version.

        Examples:
          >>> icv.explain_violations(File('constraints.ttl'),
                                     graph_uri='urn:graph')
        """
        transaction = self.conn.transaction
        url = (
            "/icv/{}/violations".format(transaction)
            if transaction
            else "/icv/violations"
        )

        with content.data() as data:
            r = self.client.post(
                url,
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
                params={"graph-uri": graph_uri},
            )

            return self.client._multipart(r)

    def convert(self, content: Content, graph_uri: Optional[str] = None) -> str:
        """
        Converts given integrity constraints to a SPARQL query.

        :param content: Integrity constraints
        :graph_uri: Named graph from which to apply constraints

        :return: SPARQL query

        .. warning::
            Deprecated: :meth:`stardog.connection.ICV.convert()` was meant as a debugging tool, and will be removed in the next major version.

        Examples:
          >>> icv.convert(File('constraints.ttl'), graph_uri='urn:graph')
        """
        with content.data() as data:
            r = self.client.post(
                "/icv/convert",
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
                params={"graph-uri": graph_uri},
            )

            return r.text

    def report(self, **kwargs) -> str:
        """
        Produces a SHACL validation report.

        :keyword str, optional shapes: SHACL shapes to validate
        :keyword str, optional shacl.shape.graphs: SHACL shape graphs to validate
        :keyword str, optional nodes: SHACL focus node(s) to validate
        :keyword str, optional countLimit: Maximum number of violations to report
        :keyword bool, optional shacl.targetClass.simple: If ``True``, ``sh:targetClass`` will be evaluated based on ``rdf:type`` triples only, without following ``rdfs:subClassOf`` relations
        :keyword str, optional shacl.violation.limit.shape: number of violation limits per SHACL shapes
        :keyword str, optional graph-uri: Named Graph
        :keyword bool, optional reasoning: If ``True``, enable reasoning.

        :return: SHACL validation report

        Examples:
          >>> icv.report()
        """

        accepted_args = [
            "shapes",
            "shacl.shape.graphs",
            "nodes",
            "countLimit",
            "shacl.targetClass.simple",
            "shacl.violation.limit.shape",
            "graph-uri",
            "reasoning",
        ]
        for arg in kwargs:
            if arg not in accepted_args:
                raise Exception("Parameter not recognized")

        kwargs["prettify"] = True
        params = urllib.parse.urlencode(kwargs)
        url = f"/icv/report?{params}"

        r = self.client.post(url)
        return r.text


class GraphQL:
    """GraphQL

    See Also:
        `Stardog Docs - GraphQL <https://docs.stardog.com/query-stardog/graphql>`_

    """

    def __init__(self, conn: Connection):
        """Initializes a GraphQL.

        Use :meth:`stardog.connection.Connection.graphql`
        instead of constructing manually.
        """
        self.conn = conn
        self.client = conn.client

    def query(self, query: str, variables: Optional[dict] = None) -> dict:
        """Executes a GraphQL query.

        :param query: GraphQL query
        :param variables: GraphQL variables.
            Keys: ``@reasoning`` (bool) to enable reasoning,
            ``@schema`` (str) to define schemas

        :return: Query results

        Examples:
          with schema and reasoning

          >>> gql.query('{ Person {name} }',
                        variables={'@reasoning': True, '@schema': 'people'})

          with named variables

          >>> gql.query(
                'query getPerson($id: Int) { Person(id: $id) {name} }',
                variables={'id': 1000})

        """
        r = self.client.post(
            "/graphql",
            json={"query": query, "variables": variables if variables else {}},
        )

        res = r.json()
        if "data" in res:
            return res["data"]

        # graphql endpoint returns valid response with errors
        raise exceptions.StardogException(res)

    def schemas(self) -> Dict:
        """Retrieves all available schemas.

        :return: All schemas
        """
        r = self.client.get("/graphql/schemas")
        return r.json()["schemas"]

    def clear_schemas(self) -> None:
        """Deletes all schemas."""
        self.client.delete("/graphql/schemas")

    def add_schema(self, name: str, content: Content) -> None:
        """Adds a schema to the database.

        :param name: Name of the schema
        :param content: Schema data

        Examples:
          >>> gql.add_schema('people', content=File('people.graphql'))
        """
        with content.data() as data:
            self.client.put("/graphql/schemas/{}".format(name), data=data)

    def schema(self, name: str) -> str:
        """Gets schema information.

        :param name: Name of the schema
        :return: GraphQL schema
        """
        r = self.client.get("/graphql/schemas/{}".format(name))
        return r.text

    def remove_schema(self, name: str) -> None:
        """Removes a schema from the database.

        :param name: Name of the schema
        """
        self.client.delete("/graphql/schemas/{}".format(name))


@contextlib.contextmanager
def _nextcontext(r):
    yield next(r)
