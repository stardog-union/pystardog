"""Administer a Stardog server.

"""

import json
from typing import Dict, List, Optional, Tuple, Union
import contextlib2
import urllib
from time import sleep
from requests.auth import AuthBase

from stardog.content import Content, ImportFile, ImportRaw, MappingFile, MappingRaw

from . import content_types as content_types
from .http import client

DEFAULT_MAPPINGS_SYNTAX = "SMS"


class Admin:
    """Admin Connection.

    This is the entry point for admin-related operations on a Stardog server.
    See Also:
        `Stardog Docs - Operating Stardog <https://docs.stardog.com/operating-stardog/>`_
    """

    def __init__(
        self,
        endpoint: Optional[str] = client.Client.DEFAULT_ENDPOINT,
        username: Optional[str] = client.Client.DEFAULT_USERNAME,
        password: Optional[str] = client.Client.DEFAULT_PASSWORD,
        auth: Optional[AuthBase] = None,
        run_as: Optional[str] = None,
    ) -> None:
        """Initializes an admin connection to a Stardog server.

        :param endpoint: URL of the Stardog server endpoint.
        :param username: Username to use in the connection.
        :param password: Password to use in the connection.
        :param auth: :class:`requests.auth.AuthBase` object. Used as an alternative authentication scheme. If not provided, HTTP Basic auth will be attempted with the ``username`` and ``password``.
        :param run_as: the user to impersonate

        .. note::
            ``auth`` and ``username``/``password`` should not be used together.  If they are, the value of ``auth`` will take precedent.

        Examples:
          >>> admin = Admin(endpoint='http://localhost:9999',
                            username='admin', password='admin')
        """
        self.client = client.Client(
            endpoint, None, username, password, auth=auth, run_as=run_as
        )
        # ensure the server is alive and at the specified location
        self.alive()

    def shutdown(self) -> None:
        """Shuts down the server."""
        self.client.post("/admin/shutdown")

    def alive(self) -> bool:
        """
        Determine whether the server is running

        :return: is the server alive?
        """
        r = self.client.get("/admin/alive")
        return r.status_code == 200

    def healthcheck(self) -> bool:
        """
        Determine whether the server is running and able to accept traffic

        :return: is the server accepting traffic?
        """
        r = self.client.get("/admin/healthcheck")
        return r.status_code == 200

    def get_prometheus_metrics(self) -> str:
        """ """
        r = self.client.get("/admin/status/prometheus")
        return r.text

    def get_server_metrics(self) -> Dict:
        """
        Returns metric information from the registry in JSON format

        :return: Server metrics

        See Also:
            `HTTP API - Server metrics <https://stardog-union.github.io/http-docs/#operation/status>`_
        """
        r = self.client.get("/admin/status")
        return r.json()

    def database(self, name: str) -> "Database":
        """Retrieves an object representing a database.

        :param name: The database name

        :return: the database
        """
        return Database(name, self.client)

    def databases(self) -> List["Database"]:
        """Retrieves all databases."""
        r = self.client.get("/admin/databases")
        databases = r.json()["databases"]
        return list(map(lambda name: Database(name, self.client), databases))

    def new_database(
        self,
        name: str,
        options: Optional[Dict] = None,
        *contents: Union[Content, Tuple[Content, str], None],
        **kwargs,
    ) -> "Database":
        """Creates a new database.

        :param name: the database name
        :param options: Dictionary with database options
        :param contents: Datasets
            to perform bulk-load with. Named graphs are made with tuples of
            Content and the name.
        :keyword copy_to_server: . If ``True``, sends the files to the Stardog server; if running as a cluster,
            data will be replicated to all nodes in the cluster.

        Examples:
            Options

            >>> admin.new_database('db', {'search.enabled': True})

            bulk-load

            >>> admin.new_database('db', {},
                                   File('example.ttl'), File('test.rdf'))

            bulk-load to named graph

            >>> admin.new_database('db', {}, (File('test.rdf'), 'urn:context'))
        """
        fmetas = []
        params = []
        copy_to_server = kwargs.get("copy_to_server", False)
        with contextlib2.ExitStack() as stack:
            for c in contents:
                content = c[0] if isinstance(c, tuple) else c
                context = c[1] if isinstance(c, tuple) else None

                # we will be opening references to many sources in a
                # single call use a stack manager to make sure they
                # all get properly closed at the end
                data = stack.enter_context(content.data())
                fname = content.name
                fmeta = {"filename": fname}

                if context:
                    fmeta["context"] = context

                fmetas.append(fmeta)
                params.append(
                    (
                        fname,
                        (
                            fname,
                            data,
                            content.content_type,
                            {"Content-Encoding": content.content_encoding},
                        ),
                    )
                )

            meta = {
                "dbname": name,
                "options": options if options else {},
                "files": fmetas,
                "copyToServer": copy_to_server,
            }

            params.append(("root", (None, json.dumps(meta), "application/json")))
            self.client.post("/admin/databases", files=params)
            return Database(name, self.client)

    def restore(
        self,
        from_path: str,
        *,
        name: Optional[str] = None,
        force: Optional[bool] = False,
    ) -> None:
        """Restore a database.

        :param from_path: the full path on the server's file system to the backup
        :param name: the name of the database to
            restore to if different from the backup
        :param force: by default, a backup will not be restored in place of an
            existing database of the same name; the ``force`` parameter should be used
            to overwrite the database

        Examples:

        .. code-block:: python
            :caption: simple restore

            admin.restore("/data/stardog/.backup/db/2019-12-01")

        .. code-block:: python
            :caption: restore the backup and overwrite ``db2`` database

            admin.restore("/data/stardog/.backup/db/2019-11-05",
                          name="db2", force=True)

        See Also:
            `Stardog Docs - Restoring a Database <https://docs.stardog.com/operating-stardog/database-administration/backup-and-restore#restoring-a-database>`_

            `HTTP API - Restore a Database <https://stardog-union.github.io/http-docs/#tag/DB-Admin/operation/restoreDatabase>`_

        """
        params = {"from": from_path, "force": force}
        if name:
            params["name"] = name

        self.client.put("/admin/restore", params=params)

    def backup_all(self, location: Optional[str] = None):
        """
        Create a backup of all databases on the server. This is also known as a **server backup**.

        :param location: where to write the server backup to on the Stardog server's file system.

        .. note::
            By default, backups are stored in the ``.backup`` directory in ``$STARDOG_HOME``,
            but you can use the ``backup.dir`` property in your ``stardog.properties`` file
            to specify a different location for backups or you can override it using the ``location`` parameter.

        """
        url = "/admin/databases/backup_all"
        if location is not None:
            params = urllib.parse.urlencode({"to": location})
            url = f"{url}?{params}"
        self.client.put(url)

    def get_all_metadata_properties(self) -> Dict:
        """
        Get information on all database metadata properties, including description and example values

        :return: Metadata properties

        See also:
            `HTTP API - Get all database metadata properties <https://stardog-union.github.io/http-docs/#tag/DB-Admin/operation/getAllMetaProperties>`_
        """
        r = self.client.get("/admin/config_properties")
        return r.json()

    def query(self, id: str) -> Dict:
        """Gets information about a running query.

        :param id: Query ID

        :return: Query information
        """
        r = self.client.get("/admin/queries/{}".format(id))
        return r.json()

    def queries(self) -> Dict:
        """Gets information about all running queries.

        :return: information about all running queries
        """
        r = self.client.get("/admin/queries")
        return r.json()["queries"]

    def kill_query(self, id: str) -> None:
        """Kills a running query.

        :param id: ID of the query to kill
        """
        self.client.delete("/admin/queries/{}".format(id))

    def stored_query(self, name: str) -> "StoredQuery":
        """Retrieves a Stored Query.

        :param name: The name of the stored query to retrieve
        """
        return StoredQuery(name, self.client)

    def stored_queries(self) -> List["StoredQuery"]:
        """Retrieves all stored queries."""
        r = self.client.get(
            "/admin/queries/stored", headers={"Accept": "application/json"}
        )
        queries = r.json()["queries"]
        return list(
            map(lambda query: StoredQuery(query["name"], self.client, query), queries)
        )

    def new_stored_query(
        self, name: str, query: str, options: Optional[Dict] = None
    ) -> "StoredQuery":
        """Creates a new Stored Query.


        :param name: the name of the stored query
        :param query: the query to store
        :param options: Additional options (e.g. ``{"shared": True, "database": "myDb" }``)

        :return: the new StoredQuery object

        Examples:

        .. code-block:: python
            :caption: Create a new stored query named ``all triples`` and make it only executable against
                the database ``mydb``.

            new_stored_query = admin.new_stored_query(
                'all triples',
                'select * where { ?s ?p ?o . }',
                { 'database': 'mydb' }
            )
        """
        if options is None:
            options = {}

        meta = {"name": name, "query": query, "creator": self.client.username}
        meta.update(options)

        self.client.post("/admin/queries/stored", json=meta)
        return StoredQuery(name, self.client)

    # TODO
    # def update_stored_query(self):
    #     """
    #     Add stored query, overwriting if a query with that name already exists
    #     https://stardog-union.github.io/http-docs/#operation/updateStoredQuery
    #     :return:
    #     """

    def clear_stored_queries(self) -> None:
        """Remove all stored queries on the server."""
        self.client.delete("/admin/queries/stored")

    # TODO
    # def new_stored_function(self):
    #     """
    #     Adds Stored Function
    #     https://stardog-union.github.io/http-docs/#operation/addStoredFunction
    #     :return:
    #     """

    # TODO
    # def stored_function(self):
    #     """
    #     Get Stored function
    #     https://stardog-union.github.io/http-docs/#operation/getStoredFunction
    #     :return:
    #     """

    # TODO
    # def stored_functions(self):
    #     """
    #     Retrieve all stored functions on the server, or optionally just the function specified in the query string parameter
    #     https://stardog-union.github.io/http-docs/#operation/exportStoredFunctions
    #     :return:
    #     """

    # TODO: Note this can delete all, or a specific stored function passed as a paramter.
    #  We might need to discuss if we are going to support both methods separatly, or depending if no param is passed, then delete all stored functions.
    # def delete_stored_functions(self):
    #     """
    #     Delete all stored functions on the server, or optionally just the function specified in the query string parameter
    #     https://stardog-union.github.io/http-docs/#operation/deleteStoredFunction
    #     :return:
    #     """

    def user(self, name: str) -> "User":
        """Retrieves a User object.

        :param name: The name of the user
        """
        return User(name, self.client)

    def users(self) -> List["User"]:
        """Retrieves all users."""
        r = self.client.get("/admin/users")
        users = r.json()["users"]
        return list(map(lambda name: User(name, self.client), users))

    def new_user(self, username: str, password: str, superuser: bool = False) -> "User":
        """Creates a new user.

        :param username: The username
        :param password: The password
        :param superuser: Create the user as a superuser. Only superusers can make other superusers.

        :return: The new User object
        """
        meta = {
            "username": username,
            "password": list(password),
            "superuser": superuser,
        }

        self.client.post("/admin/users", json=meta)
        return self.user(username)

    def role(self, name: str) -> "Role":
        """Retrieves a Role.

        :param name: The name of the role
        """
        return Role(name, self.client)

    def roles(self) -> List["Role"]:
        """Retrieves all roles."""
        r = self.client.get("/admin/roles")
        roles = r.json()["roles"]
        return list(map(lambda name: Role(name, self.client), roles))

    def new_role(self, name: str):
        """Creates a  new role.

        :param name: the name of the new role

        :return: the new Role object
        """
        self.client.post("/admin/roles", json={"rolename": name})
        return Role(name, self.client)

    def virtual_graph(self, name: str) -> "VirtualGraph":
        """Retrieves a Virtual Graph.

        :param name: The name of the virtual graph to retrieve
        """
        return VirtualGraph(name, self.client)

    def virtual_graphs(self) -> List["VirtualGraph"]:
        """Retrieves all virtual graphs."""
        r = self.client.get("/admin/virtual_graphs")
        virtual_graphs = r.json()["virtual_graphs"]
        return list(
            map(
                lambda name: VirtualGraph(name.replace("virtual://", ""), self.client),
                virtual_graphs,
            )
        )

    # TODO
    # def virtual_graphs_info(self):
    #     """
    #     List Virtual Graphs Info
    #     https://stardog-union.github.io/http-docs/#operation/virtualGraphInfos
    #
    #     :return:
    #     """

    def import_virtual_graph(
        self,
        db: str,
        mappings: Union[MappingRaw, MappingFile, str],
        named_graph: str,
        remove_all: Optional[bool],
        options: Dict,
    ) -> None:
        """Import (materialize) a virtual graph directly into the Stardog database.

        .. warning::
            **Deprecated**: :meth:`stardog.admin.Admin.materialize_virtual_graph` should be preferred.

        :param db: The database into which to import the graph
        :param mappings: New mapping contents. An empty string can be passed for autogenerated mappings.
        :param named_graph: Name of the graph to import the virtual graph into.
        :param remove_all: Should the target named graph be cleared before importing?
        :param options: Options for the new virtual graph. See `Stardog Docs - Virtual Graph Properties <https://docs.stardog.com/virtual-graphs/virtual-graph-configuration#virtual-graph-properties>`_ for all available options.

        Examples:

        .. code-block:: python
            :caption: Import a MySQL virtual graph into the ``db-name`` database using the mappings specified in ``mappings.ttl``.
                The virtual graph will be imported into the named graph ``my-graph`` and prior to the import will have its contents cleared.

            admin.import_virtual_graph(
                  'db-name',
                  mappings=File('mappings.ttl'),
                  named_graph='my-graph',
                  remove_all=True,
                  options={'jdbc.driver': 'com.mysql.jdbc.Driver'}
            )
        """

        if mappings == "":
            mappings = None

        # we kept the interface to be backward
        self.materialize_virtual_graph(
            db, mappings, None, options, named_graph, remove_all
        )

    # As indicated in the CLI, this is the new recommend way to load import virtual graph is by using COPY using the datasource.
    # Python does not support method overloading therefore, we kept the import_virtual graph name for the original code, and the
    # new one is called materialize_virtual_graph
    def materialize_virtual_graph(
        self,
        db: str,
        mappings: Union[MappingFile, MappingRaw, str],
        data_source: Optional[str] = None,
        options: Optional[Dict] = None,
        named_graph: Optional[str] = "tag:stardog:api:context:default",
        remove_all: bool = False,
    ):
        """Import (materialize) a virtual graph directly into a database.

        :param db: The database into which to import the graph
        :param mappings: New mapping contents. An empty string can be passed for autogenerated mappings.
        :param data_source: The datasource to load from
        :param options: Options for the new virtual graph, See `Stardog Docs - Virtual Graph Properties <https://docs.stardog.com/virtual-graphs/virtual-graph-configuration#virtual-graph-properties>`_ for all available options.
        :param named_graph: Name of the graph into which import the virtual graph.
        :param remove_all: Should the target named graph be cleared before importing?

        .. note::
            ``data_source`` or ``options`` must be provided.
        """

        assert (
            data_source is not None or options is not None
        ), "Either parameter 'data_source' or 'options' must be provided"

        if mappings is None:
            mappings = ""
        else:
            if hasattr(mappings, "syntax") and mappings.syntax:
                if options:
                    options["mappings.syntax"] = mappings.syntax
                else:
                    options = {"mappings.syntax": mappings.syntax}
            else:
                if options:
                    options["mappings.syntax"] = DEFAULT_MAPPINGS_SYNTAX
                else:
                    options = {
                        "mappings.syntax": DEFAULT_MAPPINGS_SYNTAX
                    }  # this is the default of the original method

            with mappings.data() as data:
                if hasattr(data, "read"):
                    r = data.read()
                    mappings = r.decode() if hasattr(r, "decode") else r
                else:
                    mappings = data

        meta = {
            "db": db,
            "mappings": mappings,
            "named_graph": named_graph,
            "remove_all": remove_all,
            "options": options,
        }

        if data_source:
            meta["data_source"] = data_source

        r = self.client.post("/admin/virtual_graphs/import_db", json=meta)

    def new_virtual_graph(
        self,
        name: str,
        mappings: Union[MappingFile, MappingRaw, None] = None,
        options: Optional[Dict] = None,
        datasource: Optional[str] = None,
        db: Optional[str] = None,
    ) -> "VirtualGraph":
        """Creates a new Virtual Graph.

        :param name: The name of the virtual graph.
        :param mappings: New mapping contents. If ``None`` provided, mappings will be autogenerated.
        :param options: Options for the new virtual graph. If ``None`` provided, then a ``datasource`` must be specified.
        :param datasource: Name of the datasource to use. If ``None`` provided, ``options`` with a ``datasource`` key must be set.
        :param db: Name of the database to associate the virtual graph. If ``None`` provided, the virtual graph will be associated with all databases.

        :return: the new VirtualGraph

        Examples:

        .. code-block:: python
            :caption: Create a new virtual graph named ``users`` and associate it with all databases. The SMS2 mappings are provided in the ``mappings.ttl`` file.

            new_vg = admin.new_virtual_graph(
                         name='users',
                         mappings=MappingFile('mappings.ttl','SMS2'),
                         datasource='my_datasource'
                    )
        """

        if mappings is None:
            mappings = ""
        elif mappings != "":  # This check is to be backward compatible if used pass "".
            if hasattr(mappings, "syntax") and mappings.syntax:
                if options:
                    options["mappings.syntax"] = mappings.syntax
                else:
                    options = {"mappings.syntax": mappings.syntax}
            else:
                if options:
                    options["mappings.syntax"] = DEFAULT_MAPPINGS_SYNTAX
                else:
                    options = {
                        "mappings.syntax": DEFAULT_MAPPINGS_SYNTAX
                    }  # this is the default of the original method

            with mappings.data() as data:
                if hasattr(data, "read"):
                    r = data.read()
                    mappings = r.decode() if hasattr(r, "decode") else r
                else:
                    mappings = data

        meta = {"name": name, "mappings": mappings}
        if options is not None:
            meta["options"] = options

        if datasource is not None:
            meta["data_source"] = datasource

        if db is not None:
            meta["db"] = db

        self.client.post("/admin/virtual_graphs", json=meta)
        return VirtualGraph(name, self.client)

    def import_file(
        self,
        db: str,
        mappings: Union[MappingRaw, MappingFile],
        input_file: Union[ImportFile, ImportRaw],
        options: Optional[Dict] = None,
        named_graph: Optional[str] = None,
    ) -> bool:
        """Import a JSON or CSV file.

        :param db: Name of the database to import the data
        :param mappings: Mappings specifying how to import the data contained in the CSV/JSON.
        :param input_file: the JSON or CSV file to import
        :param options: Options for the import.
        :param named_graph: The named graph to import the mapped CSV/JSON into.

        :return: was the import successful?
        """

        if mappings is not None:
            if mappings.syntax:
                if options:
                    options["mappings.syntax"] = mappings.syntax
                else:
                    options = {"mappings.syntax": mappings.syntax}

            with mappings.data() as data:
                if hasattr(data, "read"):
                    r = data.read()
                    mappings = r.decode() if hasattr(r, "decode") else r
                else:
                    mappings = data

        if input_file is not None:
            if input_file.separator:
                if options:
                    options["csv.separator"] = input_file.separator
                else:
                    options = {"csv.separator": input_file.separator}

        payload = {"database": db, "mappings": mappings}

        if options is not None:
            payload["options"] = "\n".join(
                ["%s=%s" % (k, v) for (k, v) in options.items()]
            )
        else:
            payload["options"] = ""

        if named_graph is not None:
            payload["named_graph"] = named_graph

        payload["input_file_type"] = input_file.input_type
        payload["input_file_iri"] = input_file.iri

        with input_file.data() as data:
            r = self.client.post(
                "/admin/virtual_graphs/import",
                data=payload,
                files={
                    "input_file": (
                        input_file.name,
                        data,
                        input_file.content_type,
                        input_file.content_encoding,
                    )
                },
            )

        return r.ok

    def datasource(self, name: str) -> "DataSource":
        """Retrieves an object representing a DataSource.

        :param name: The name of the data source
        """
        return DataSource(name, self.client)

    def datasources(self) -> List["DataSource"]:
        """Retrieves all data sources."""
        r = self.client.get("/admin/data_sources")
        data_sources = r.json()["data_sources"]
        return list(map(lambda name: DataSource(name, self.client), data_sources))

    def datasources_info(self) -> List[Dict]:
        """List all data sources with their details

        :return: a list of data sources with their details
        """

        r = self.client.get("/admin/data_sources/list")
        return r.json()["data_sources"]

    def new_datasource(self, name: str, options: Dict) -> "DataSource":
        """Creates a new DataSource.

        :param name: The name of the data source
        :param options: Data Source options

        :return: The new DataSource object
        """

        if options is None:
            options = {}

        meta = {"name": name, "options": options}

        self.client.post("/admin/data_sources", json=meta)
        return DataSource(name, self.client)

    def get_server_properties(self) -> Dict:
        """Get the value of any set server-level properties

        :return: server properties
        """

        r = self.client.get("/admin/properties")
        return r.json()

    # TODO
    # def set_server_properties(self):
    #     """
    #     Set the value of specific server properties
    #     https://stardog-union.github.io/http-docs/#operation/setProperty
    #     :return:
    #     """

    def validate(self) -> bool:
        """Validates an admin connection.

        :return: whether the connection is valid or not
        """
        r = self.client.get("/admin/users/valid")
        return r.status_code == 200

    def cluster_list_standby_nodes(self) -> Dict:
        """
        List standby nodes

        :return: all standby nodes in the cluster
        """
        r = self.client.get("/admin/cluster/standby/registry")
        return r.json()

    def cluster_join(self) -> None:
        """
        Instruct a standby node to join its cluster as a full node
        """
        self.client.put("/admin/cluster/standby/join")

    def standby_node_pause_status(self) -> Dict:
        """
        Get the pause status of a standby node

        :return: Pause status of a standby node, possible values are: ``WAITING``, ``SYNCING``, ``PAUSING``, ``PAUSED``

        See also:
            `HTTP API - Get Paused State of Standby Node <https://stardog-union.github.io/http-docs/#operation/getPauseState>`_

        """
        r = self.client.get("/admin/cluster/standby/pause")
        return r.json()

    def standby_node_pause(self, pause: bool) -> bool:
        """
        Pause/Unpause standby node

        :param pause: ``True`` should be provided to pause the standby node. ``False`` should be provided to unpause.
        :return: whether the pause status was successfully changed or not.
        """
        if pause:
            r = self.client.put("/admin/cluster/standby/pause?pause=true")
        else:
            r = self.client.put("/admin/cluster/standby/pause?pause=false")
        return r.status_code == 200

    def cluster_revoke_standby_access(self, registry_id: str) -> None:
        """
        Instruct a standby node to stop syncing

        :param registry_id: ID of the standby node.
        """
        self.client.delete("/admin/cluster/standby/registry/" + registry_id)

    def cluster_start_readonly(self) -> None:
        """
        Start read only mode
        """
        self.client.put("/admin/cluster/readonly")

    def cluster_stop_readonly(self) -> None:
        """
        Stops read only mode
        """
        self.client.delete("/admin/cluster/readonly")

    def cluster_coordinator_check(self) -> bool:
        """
        Determine if a specific cluster node is the cluster coordinator

        :return: whether the node is a coordinator or not.
        """
        r = self.client.get("/admin/cluster/coordinator")
        return r.status_code == 200

    # TODO:
    # def cluster_diagnostics_report(self):
    #     """
    #     Get cluster diagnostics report
    #     https://stardog-union.github.io/http-docs/#operation/generateClusterDiagnosticsReport
    #     :return:
    #     """
    #     r = self.client.post('/admin/cluster/diagnostics')
    #     return r

    def cluster_status(self) -> Dict:
        """Prints status information for each node
        in the cluster

        :return: status information about each node in the cluster
        """
        r = self.client.get("/admin/cluster/status")
        return r.json()

    def cluster_info(self) -> Dict:
        """Prints info about the nodes in the Stardog
        cluster.

        :return: information about nodes in the cluster
        """
        r = self.client.get("/admin/cluster")
        return r.json()

    def cluster_shutdown(self) -> bool:
        """
        Shutdown all nodes in the cluster

        :return: whether the cluster was shutdown successfully or not.
        """
        r = self.client.post("/admin/shutdownAll")
        return r.status_code == 200

    def cache(self, name: str) -> "Cache":
        """Retrieve an object representing a cached dataset.

        :param name: the name of the cache to retrieve
        """
        return Cache(name, self.client)

    def cache_status(self, *names: str) -> List[Dict]:
        """Retrieves the status of one or more cached graphs.

        :param names: Names of the cached graphs to retrieve status for

        :return: list of statuses
        """
        return self.client.post("/admin/cache/status", json=names).json()

    def cached_status(self) -> List["Cache"]:
        """Retrieves all cached graphs."""
        r = self.client.get("/admin/cache/status")
        cache_names = [cache_name["name"] for cache_name in r.json()]
        return list(map(lambda name: Cache(name, self.client), cache_names))

    def cached_queries(self) -> List["Cache"]:
        """
        Retrieves all cached queries.

        .. warning::
            This method is deprecated in Stardog 8+

        :return: cached queries

        """
        r = self.client.get("/admin/cache/queries")
        cache_names = [cache_name["name"] for cache_name in r.json()]
        return list(map(lambda name: Cache(name, self.client), cache_names))

    def cached_graphs(self) -> List["Cache"]:
        """Retrieves all cached graphs."""
        r = self.client.get("/admin/cache/graphs")
        cache_names = [cache_name["name"] for cache_name in r.json()]
        return list(map(lambda name: Cache(name, self.client), cache_names))

    def new_cached_query(
        self,
        name: str,
        target: str,
        query: str,
        database: str,
        refresh_script: Optional[str] = None,
        register_only: bool = False,
    ) -> "Cache":
        """Creates a new cached query.

        .. warning::
            This method is deprecated in Stardog 8+

        :param name: The name (URI) of the cached query
        :param target: The name (URI) of the cache target
        :param query: The query to cache
        :param database: The name of the database
        :param refresh_script: A SPARQL insert query to run
          when refreshing the cache
        :param register_only: If ``True``, register a
          cached dataset without loading data from the source graph
          or query into the cache target's databases

        :return: the new Cache
        """

        params = {
            "name": name,
            "target": target,
            "database": database,
            "query": query,
        }

        if refresh_script:
            params["refreshScript"] = refresh_script

        if register_only:
            params["registerOnly"] = True

        self.client.post("/admin/cache", json=params)
        return Cache(name, self.client)

    def new_cached_graph(
        self,
        name: str,
        target: str,
        graph: str,
        database: Optional[str] = None,
        refresh_script: Optional[str] = None,
        register_only: bool = False,
    ) -> "Cache":
        """Creates a new cached graph.

        :param name: The name (URI) of the cached query
        :param target: The name (URI) of the cache target
        :param graph: The name of the graph to cache
        :param database: The name of the database. Optional for virtual graphs, required for named graphs.
        :param refresh_script: An optional SPARQL update query to run when refreshing the cache.
        :param register_only: An optional value that if ``True``, register a cached dataset without loading data from the source graph or query into the cache target's databases.


        :return: The new Cache
        """

        params = {
            "name": name,
            "target": target,
            "graph": graph,
        }

        if database:
            params["database"] = database

        if refresh_script:
            params["refreshScript"] = refresh_script

        if register_only:
            params["registerOnly"] = True

        self.client.post("/admin/cache", json=params)
        return Cache(name, self.client)

    def cache_targets(self) -> List["CacheTarget"]:
        """Retrieves all cache targets."""
        r = self.client.get("/admin/cache/target")
        return list(
            map(lambda target: CacheTarget(target["name"], self.client), r.json())
        )

    def new_cache_target(
        self,
        name: str,
        hostname: str,
        port: int,
        username: str,
        password: str,
        use_existing_db: bool = False,
    ) -> "CacheTarget":
        """Creates a new cache target.

        :param name: The name of the cache target
        :param hostname: The hostname of the cache target server
        :param port: The port of the cache target server
        :param username: The username for the cache target
        :param password: The password for the cache target
        :param use_existing_db: If ``True``, check for an existing cache database to use before creating a new one

        :return: the new CacheTarget
        """
        params = {
            "name": name,
            "hostname": hostname,
            "port": port,
            "username": username,
            "password": password,
        }

        if use_existing_db:
            params["useExistingDb"] = True

        self.client.post("/admin/cache/target", json=params)
        return CacheTarget(name, self.client)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()


class Database:
    """Represents a Stardog database."""

    def __init__(self, name, client):
        """Initializes a Database.

        Use :meth:`stardog.admin.Admin.database`,
        :meth:`stardog.admin.Admin.databases`, or
        :meth:`stardog.admin.Admin.new_database` instead of
        constructing manually.
        """
        self.database_name = name
        self.client = client
        self.path = "/admin/databases/{}".format(name)

        # this checks for existence by throwing an exception if the resource does not exist
        self.client.get(self.path + "/options")

    @property
    def name(self) -> str:
        """The name of the database."""
        return self.database_name

    def get_options(self, *options: str) -> Dict:
        """Get the value of specific metadata options for a database

        :param options: Database option names

        :return: Database options

        Examples
          >>> db.get_options('search.enabled', 'spatial.enabled')
        """
        # transform into {'option': None} dict
        meta = dict([(x, None) for x in options])

        r = self.client.put(self.path + "/options", json=meta)
        return r.json()

    def get_all_options(self) -> Dict:
        """Get the value of every metadata option for a database

        :return: All database metadata
        """
        r = self.client.get(self.path + "/options")
        return r.json()

    def set_options(self, options: Dict) -> None:
        """Sets database options.

        :param options: Database options

        .. note::
            The database must be offline to set some options (e.g. ``search.enabled``).

        Examples:
            >>> db.set_options({'spatial.enabled': False})
        """

        r = self.client.post(self.path + "/options", json=options)
        return r.status_code == 200

    def optimize(self) -> None:
        """Optimizes a database."""
        self.client.put(self.path + "/optimize")

    def verify(self) -> None:
        """verifies a database."""
        self.client.post(self.path + "/verify")

    def repair(self) -> bool:
        """Attempt to recover a corrupted database.

        .. note::
            The database must be offline.

        :return: whether the database was successfully repaired or not
        """
        r = self.client.post(self.path + "/repair")
        return r.status_code == 200

    def backup(self, *, to: Optional[str] = None) -> None:
        """Create a backup of a database on the server.

        :param to: specify a path on the Stardog server's file system to store
            the backup

        See Also:
            `Stardog Docs - Backup a Database <https://docs.stardog.com/operating-stardog/database-administration/backup-and-restore>`_
        """
        params = {"to": to} if to else {}
        self.client.put(self.path + "/backup", params=params)

    def online(self) -> None:
        """Sets a database to online state."""
        self.client.put(self.path + "/online")

    def offline(self) -> None:
        """Sets a database to offline state."""
        self.client.put(self.path + "/offline")

    def copy(self, to):
        """Makes a copy of this database under another name.

        .. warning::
            This method is deprecated and not valid for Stardog versions 6+.

        The database must be offline.

        Args:
          to (str): Name of the new database to be created

        Returns:
          Database: The new Database
        """
        self.client.put(self.path + "/copy", params={"to": to})
        return Database(to, self.client)

    def drop(self) -> None:
        """Drops the database."""
        self.client.delete(self.path)

    def namespaces(self) -> Dict:
        """
        Retrieve the namespaces stored in the database

        :return: A dict listing the prefixes and IRIs of the stored namespaces

        See also:
            `HTTPI API - Get Namespaces <https://stardog-union.github.io/http-docs/#operation/getNamespaces>`_
        """
        r = self.client.get(f"/{self.database_name}/namespaces")
        return r.json()["namespaces"]

    def import_namespaces(self, content: Content) -> Dict:
        """
        Imports namespace prefixes from an RDF file
        that contains prefix declarations into the database, overriding any
        previous mappings for those prefixes. Only the prefix declarations in
        the file are processed, the rest of the file is not parsed.

        :param content: RDF File containing prefix declarations

        :return: Dictionary with all namespaces after import
        """

        with content.data() as data:
            r = self.client.post(
                "/" + self.database_name + "/namespaces",
                data=data,
                headers={
                    "Content-Type": content.content_type,
                    "Content-Encoding": content.content_encoding,
                },
            )

        return r.json()

    def add_namespace(self, prefix: str, iri: str) -> bool:
        """Adds a specific namespace to a database

        :param prefix: the prefix of the namespace to be added
        :param iri: the iri associated with the ``prefix`` to be added

        :return: whether the operation succeeded or not.
        """

        # easy way to check if a namespace already exists
        current_namespaces = self.namespaces()
        for namespace in current_namespaces:
            if prefix == namespace["prefix"]:
                raise Exception(
                    f"Namespace already exists for this database: {namespace}"
                )

        namespaces = self.get_options("database.namespaces")["database.namespaces"]
        namespace_to_append = f"{prefix}={iri}"
        namespaces.append(namespace_to_append)
        result = self.set_options({"database.namespaces": namespaces})
        return result

    def remove_namespace(self, prefix: str) -> bool:
        """Removes a specific namespace from a database

        :param prefix: the prefix of the namespace to be removed

        :return: whether the operation succeeded or not.
        """

        # easy way to check if a namespace already exists
        current_namespaces = self.namespaces()
        for namespace in current_namespaces:
            if prefix == namespace["prefix"]:
                namespaces = self.get_options("database.namespaces")[
                    "database.namespaces"
                ]
                namespace_to_remove = f"{prefix}={namespace['name']}"
                namespaces.remove(namespace_to_remove)
                result = self.set_options({"database.namespaces": namespaces})
                return result

        raise Exception(f"Namespace does not exists for this database: {namespace}")

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name


class StoredQuery:
    """Stored Query

    See Also:
        `Stardog Docs - Stored Queries <https://docs.stardog.com/operating-stardog/database-administration/stored-queries>`_
    """

    def __init__(
        self, name: str, client: client.Client, details: Optional[Dict] = None
    ):
        """Initializes a stored query.

        Use :meth:`stardog.admin.Admin.stored_query`,
        :meth:`stardog.admin.Admin.stored_queries`, or
        :meth:`stardog.admin.Admin.new_stored_query` instead of
        constructing manually.
        """
        self.query_name = name
        self.client = client
        self.path = "/admin/queries/stored/{}".format(name)

        # this checks for existence by throwing an exception if the resource does not exist
        self.client.get(self.path)

        # We only need to call __refresh() if the details are not provided
        if details is not None and isinstance(details, dict):
            self.details = details
        else:
            self.details = {}
            self.__refresh()

    def __refresh(self):
        details = self.client.get(self.path, headers={"Accept": "application/json"})
        self.details.update(details.json()["queries"][0])

    @property
    def name(self) -> str:
        """The name of the stored query."""
        return self.query_name

    @property
    def description(self) -> str:
        """The description of the stored query."""
        return self.details["description"]

    @property
    def creator(self) -> str:
        """The creator of the stored query."""
        return self.details["creator"]

    @property
    def database(self) -> str:
        """The database the stored query applies to."""
        return self.details["database"]

    @property
    def query(self) -> str:
        """The text of the stored query."""
        return self.details["query"]

    @property
    def shared(self) -> bool:
        """The value of the shared property."""
        return self.details["shared"]

    @property
    def reasoning(self) -> bool:
        """The value of the reasoning property."""
        return self.details["reasoning"]

    def update(self, **options) -> None:
        """Updates the Stored Query.

        Args:
          **options (str): Named arguments to update.

        Examples:
            Update description

            >>> stored_query.update(description='this query finds all the relevant...')
        """
        options["name"] = self.query_name
        for opt in ["query", "creator"]:
            if opt not in options:
                options[opt] = self.__getattribute__(opt)

        self.client.put("/admin/queries/stored", json=options)
        self.__refresh()

    def delete(self) -> None:
        """Deletes the Stored Query."""
        self.client.delete(self.path)

    def __eq__(self, other):
        return self.name == other.name


class User:
    """Represents a Stardog user"""

    def __init__(self, name: str, client: client.Client):
        """Initializes a User.

        Use :meth:`stardog.admin.Admin.user`,
        :meth:`stardog.admin.Admin.users`, or
        :meth:`stardog.admin.Admin.new_user` instead of
        constructing manually.
        """
        self.username = name
        self.client = client
        self.path = "/admin/users/{}".format(name)
        # this checks for existence by throwing an exception if the resource does not exist
        self.client.get(self.path)

    @property
    def name(self) -> str:
        """The username."""
        return self.username

    def set_password(self, password: str) -> None:
        """Sets a new password.

        :param password: the new password for the user

        """
        self.client.put(self.path + "/pwd", json={"password": password})

    def is_enabled(self) -> bool:
        """Checks if the user is enabled.

        :return: whether the user is enabled or not
        """
        r = self.client.get(self.path + "/enabled")
        return bool(r.json()["enabled"])

    def set_enabled(self, enabled: bool) -> None:
        """Enables or disables the user.

        :param enabled: Desired state. ``True`` for enabled, ``False`` for disabled.
        """
        self.client.put(self.path + "/enabled", json={"enabled": enabled})

    def is_superuser(self) -> bool:
        """Checks if the user is a superuser.

        :return: whether the user is a superuser or not.
        """
        r = self.client.get(self.path + "/superuser")
        return bool(r.json()["superuser"])

    def roles(self) -> List["Role"]:
        """Gets all the User's roles."""
        r = self.client.get(self.path + "/roles")
        roles = r.json()["roles"]
        return list(map(lambda name: Role(name, self.client), roles))

    def add_role(self, role: Union["Role", str]) -> None:
        """Adds an existing role to the user.

        :param role: The :class:`stardog.admin.Role` or name of the role to add

        Examples:
            >>> user.add_role('reader')
            >>> user.add_role(admin.role('reader'))
        """
        self.client.post(self.path + "/roles", json={"rolename": role})

    def set_roles(self, *roles: Union[str, "Role"]) -> None:
        """Sets the roles of the user.

        :param roles: The :class:`stardog.admin.Role` (s) or name of the role(s) to add to the user

        Examples
            >>> user.set_roles('reader', admin.role('writer'))
        """
        roles = list(map(self.__rolename, roles))
        self.client.put(self.path + "/roles", json={"roles": roles})

    def remove_role(self, role: Union[str, "Role"]) -> None:
        """Removes a role from the user.

        :param role: The :class:`stardog.admin.Role` or name of the role to remove

        Examples
            >>> user.remove_role('reader')
            >>> user.remove_role(admin.role('reader'))
        """
        self.client.delete(self.path + "/roles/" + role)

    def delete(self) -> None:
        """Deletes the user."""
        self.client.delete(self.path)

    def permissions(self) -> Dict:
        """Gets the user permissions.

        See Also:
            `Stardog Docs - Permissions <https://docs.stardog.com/operating-stardog/security/security-model#permissions>`_

        :return: user permissions
        """
        r = self.client.get("/admin/permissions/user/{}".format(self.name))
        return r.json()["permissions"]

    def add_permission(self, action: str, resource_type: str, resource: str) -> None:
        """Add a permission to the user.

        :param action: Action type (e.g., ``read``, ``write``)
        :param resource_type: Resource type (e.g., ``user``, ``db``)
        :param resource: Target resource (e.g., ``username``, ``*``)

        See Also:
            `Stardog Docs - Grant Permissions to a User <https://docs.stardog.com/operating-stardog/security/managing-users-and-roles#grant-explicit-permissions-to-a-user>`_

            `HTTP API - Grant permission to a User <https://stardog-union.github.io/http-docs/#tag/Permissions/operation/addUserPermission>`_

        Examples
            >>> user.add_permission('read', 'user', 'username')
            >>> user.add_permission('write', '*', '*')
        """
        meta = {
            "action": action,
            "resource_type": resource_type,
            "resource": [resource],
        }
        self.client.put("/admin/permissions/user/{}".format(self.name), json=meta)

    def remove_permission(self, action: str, resource_type: str, resource: str):
        """Removes a permission from the user.

        :param action: Action type (e.g., ``read``, ``write``)
        :param resource_type: Resource type (e.g., ``user``, ``db``)
        :param resource: Target resource (e.g., ``username``, ``*``)

        See Also:
            `HTTP API - Revoke User Permission <https://stardog-union.github.io/http-docs/#tag/Permissions/operation/deleteUserPermission>`_

        Examples:
            >>> user.remove_permission('read', 'user', 'username')
            >>> user.remove_permission('write', '*', '*')

        """
        meta = {
            "action": action,
            "resource_type": resource_type,
            "resource": [resource],
        }

        self.client.post(
            "/admin/permissions/user/{}/delete".format(self.name), json=meta
        )

    def effective_permissions(self) -> Dict:
        """Gets the user's effective permissions.

        :return: User's effective permissions
        """
        r = self.client.get("/admin/permissions/effective/user/" + self.name)
        return r.json()["permissions"]

    def __rolename(self, role):
        return role.name if isinstance(role, Role) else role

    def __eq__(self, other):
        return self.name == other.name


class Role:
    """Role

    See Also:
        `Stardog Docs - Authorization <https://docs.stardog.com/operating-stardog/security/security-model#authorization>`_
    """

    def __init__(self, name, client):
        """Initializes a Role.

        Use :meth:`stardog.admin.Admin.role`,
        :meth:`stardog.admin.Admin.roles`, or
        :meth:`stardog.admin.Admin.new_role` instead of
        constructing manually.
        """
        self.role_name = name
        self.client = client
        self.path = "/admin/roles/{}".format(name)
        # this checks for existence by throwing an exception if the resource does not exist
        self.client.get(f"{self.path}/users")

    @property
    def name(self):
        """The name of the Role."""
        return self.role_name

    def users(self):
        """Lists the users for this role.

        Returns:
          list[User]
        """
        r = self.client.get(self.path + "/users")
        users = r.json()["users"]
        return list(map(lambda name: User(name, self.client), users))

    def delete(self, force=None):
        """Deletes the role.

        Args:
          force (bool): Force deletion of the role
        """
        self.client.delete(self.path, params={"force": force})

    def permissions(self):
        """Gets the role permissions.

        See Also:
            `HTTP API - Get Role Permissions <https://stardog-union.github.io/http-docs/#tag/Permissions/operation/getRolePermissions>`_

        Returns:
          dict: Role permissions
        """
        r = self.client.get("/admin/permissions/role/{}".format(self.name))
        return r.json()["permissions"]

    def add_permission(self, action, resource_type, resource):
        """Adds a permission to the role.

        See Also:
            `Stardog Docs - Grant Permissions to a Role <https://docs.stardog.com/operating-stardog/security/managing-users-and-roles#grant-permissions-to-a-role>`_

            `HTTP API - Grant Permission to Role <https://stardog-union.github.io/http-docs/#tag/Permissions/operation/addRolePermission>`_

        Args:
          action (str): Action type (e.g., 'read', 'write')
          resource_type (str): Resource type (e.g., 'user', 'db')
          resource (str): Target resource (e.g., 'username', '*')

        Examples:
            >>> role.add_permission('read', 'user', 'username')
            >>> role.add_permission('write', '*', '*')
        """
        meta = {
            "action": action,
            "resource_type": resource_type,
            "resource": [resource],
        }

        self.client.put("/admin/permissions/role/{}".format(self.name), json=meta)

    def remove_permission(self, action, resource_type, resource):
        """Removes a permission from the role.

        See Also:
            `HTTP API - Revoke Role Permission <https://stardog-union.github.io/http-docs/#tag/Permissions/operation/deleteRolePermission>`_

        Args:
          action (str): Action type (e.g., 'read', 'write')
          resource_type (str): Resource type (e.g., 'user', 'db')
          resource (str): Target resource (e.g., 'username', '*')

        Examples:
            >>> role.remove_permission('read', 'user', 'username')
            >>> role.remove_permission('write', '*', '*')
        """
        meta = {
            "action": action,
            "resource_type": resource_type,
            "resource": [resource],
        }

        self.client.post(
            "/admin/permissions/role/{}/delete".format(self.name), json=meta
        )

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name


class VirtualGraph:
    """Virtual Graph

    See Also:
       `Stardog Docs - Virtual Graphs <https://docs.stardog.com/virtual-graphs/>`_
    """

    def __init__(self, name, client):
        """Initializes a virtual graph.

        Use :meth:`stardog.admin.Admin.virtual_graph`,
        :meth:`stardog.admin.Admin.virtual_graphs`, or
        :meth:`stardog.admin.Admin.new_virtual_graph` instead of
        constructing manually.
        """

        self.graph_name = name
        self.path = "/admin/virtual_graphs/{}".format(name)
        self.client = client

        # this checks for existence by throwing an exception if the resource does not exist
        self.client.get(f"{self.path}/info")

    @property
    def name(self) -> str:
        """The name of the virtual graph."""
        return self.graph_name

    def update(
        self,
        name: str,
        mappings: Content,
        options: Dict = {},
        datasource: Optional[str] = None,
        db: Optional[str] = None,
    ) -> None:
        """Updates the Virtual Graph.

        :param name: The new name
        :param mappings: New mapping contents
        :param options: New virtual graph options
        :param datasource: new data source for the virtual graph
        :param db: the database to associate with the virtual graph

        Examples:

        .. code-block:: python

            vg.update(
                    name='users',
                    mappings=File('mappings.ttl'),
                    options={'jdbc.driver': 'com.mysql.jdbc.Driver'}
            )
        """
        if mappings:
            with mappings.data() as data:
                mappings = data.read().decode() if hasattr(data, "read") else data

        meta = {}
        meta["name"] = name
        meta["mappings"] = mappings
        if options is not None:
            meta["options"] = options

        if datasource is not None:
            meta["data_source"] = datasource
        else:
            meta["data_source"] = self.get_datasource()

        if db is not None:
            meta["db"] = db
        else:
            meta["db"] = self.get_database()

        self.client.put(self.path, json=meta)
        self.graph_name = name

    def delete(self) -> None:
        """Deletes the virtual graph."""
        self.client.delete(self.path)

    def options(self) -> Dict:
        """Gets virtual graph options."""
        r = self.client.get(self.path + "/options")
        return r.json()["options"]

    def info(self) -> Dict:
        """Gets virtual graph info."""

        r = self.client.get(self.path + "/info")
        return r.json()["info"]

    # should return object or name?
    def get_datasource(self) -> str:
        """Gets datasource associated with the virtual graph

        :return: datasource name with ``data-source://`` prefix removed
        """
        return self.info()["data_source"].replace("data-source://", "")

    # should return object or name?
    def get_database(self) -> str:
        """Gets database associated with the virtual graph.

        :return: the database name
        """

        r = self.client.get(self.path + "/database")
        return r.text

    def mappings_string(self, syntax: str = DEFAULT_MAPPINGS_SYNTAX):
        """Returns graph mappings from virtual graph

        :param syntax: The desired syntax of the mappings (``'STARDOG'``, ``'R2RML'``, or ``'SMS2'``).

        :return: Mappings in desired ``syntax``
        """
        r = self.client.get(f"{self.path}/mappingsString/{syntax}")
        return r.content

    # Should test this. The docs state the path is /mappingsString, but this one goes to /mappings.
    # Either the docs, or this implementation is wrong.
    def mappings(self, content_type: str = content_types.TURTLE) -> bytes:
        """Gets the Virtual Graph mappings

        .. warning::
            **Deprecated**: :meth:`stardog.admin.VirtualGraph.mappings_string` should be preferred.

        :param content_type: Content type for mappings.

        :return: Mappings in given content type
        """
        r = self.client.get(self.path + "/mappings", headers={"Accept": content_type})
        return r.content

    def available(self) -> bool:
        """Checks if the virtual graph is available.

        :return: whether the virtual graph is available or not
        """
        r = self.client.get(self.path + "/available")
        return bool(r.json())

    # TODO
    # def online(self):
    #     """
    #     Online virtual graph
    #     https://stardog-union.github.io/http-docs/#operation/onlineVG
    #     :return:
    #     """

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name


class DataSource:
    """Initializes a DataSource

    See Also:
        `Stardog Docs - Data Sources <https://docs.stardog.com/virtual-graphs/data-sources>`_
    """

    def __init__(self, name: str, client: client.Client):
        """Initializes a DataSource.

        Use :meth:`stardog.admin.Admin.data_source`,
        :meth:`stardog.admin.Admin.data_sources`, or
        :meth:`stardog.admin.Admin.new_data_source` instead of
        constructing manually.
        """

        self.data_source_name = name
        self.path = "/admin/data_sources/{}".format(name)
        self.client = client

        # this checks for existence by throwing an exception if the resource does not exist
        self.client.get(f"{self.path}/info")

    @property
    def name(self) -> str:
        """The name of the data source."""
        return self.data_source_name

    def available(self) -> bool:
        """Checks if the data source is available.

        :return: whether the data source is available or not
        """
        r = self.client.get(self.path + "/available")
        return bool(r.json())

    def refresh_count(self, meta: Optional[Dict] = None) -> None:
        """Refresh table row-count estimates

        :param meta: dict containing the table to refresh. Examples: ``{"name": "catalog.schema.table"}``,
            ``{"name": "schema.table"}``, ``{"name": "table"}``

        See also:
            `HTTP API - Refresh table row-count estimates  <https://stardog-union.github.io/http-docs/#tag/Data-Sources/operation/refreshCounts>`_
        """

        if meta is None:
            meta = {}

        self.client.post(self.path + "/refresh_counts", json=meta)

    def update(self, options: Optional[Dict] = None, force: bool = False) -> None:
        """Update data source

        :param options: Dict with data source options
        :param force: a data source will not be updated while in use unless ``force=True``

        Examples:
            >>> admin.update({"sql.dialect": "MYSQL"})
            >>> admin.update({"sql.dialect": "MYSQL"}, force=True)

        See Also:
            `Stardog Docs - DataSource options <https://docs.stardog.com/virtual-graphs/data-sources/data-source-configuration#configuration-options>`_
        """
        if options is None:
            options = {}

        meta = {"options": options, "force": force}

        self.client.put(self.path, json=meta)

    def delete(self) -> None:
        """Deletes a data source"""

        self.client.delete(self.path)

    def online(self) -> None:
        """Online a data source"""

        self.client.post(self.path + "/online")

    def info(self) -> Dict:
        """Get data source info

        :return: data source information
        """

        r = self.client.get(self.path + "/info")
        return r.json()["info"]

    def refresh_metadata(self, meta: Optional[Dict] = None) -> None:
        """Refresh metadata for one or all tables that are accessible to a data source. Will clear the saved metadata for a data source
        and reload all of its dependent virtual graphs with fresh metadata.

        :param meta: dict containing the table to refresh. Examples: ``{"name": "catalog.schema.table"}``,
            ``{"name": "schema.table"}``, ``{"name": "table"}``

        See also:
            `Stardog Docs - Refreshing Data Source Metadata <https://docs.stardog.com/virtual-graphs/data-sources/#refreshing-metadata>`_

            `HTTP API - Refresh Data Source Metadata  <https://stardog-union.github.io/http-docs/#tag/Data-Sources/operation/refreshMetadata>`_

            `Stardog CLI - stardog-admin data-source refresh-metadata <https://docs.stardog.com/stardog-admin-cli-reference/data-source/data-source-refresh-metadata>`_
        """

        if meta is None:
            meta = {}

        self.client.post(self.path + "/refresh_metadata", json=meta)

    def share(self) -> None:
        """Share a private data source. When a virtual graph is created without specifying a data source name,
        a private data source is created for that, and only that virtual graph. This methods makes the data
        source available to other virtual graphs, as well as decouples the data source life cycle from the
        original virtual graph."""

        self.client.post(self.path + "/share")

    def get_options(self) -> List[Dict]:
        """Get data source options"""

        r = self.client.get(self.path + "/options")
        return r.json()["options"]

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name


# TODO
# We could get rid of this class, and the delete method here as admin.delete_stored_functions() can take a single stored function
# and mimic this behaviour. This is intentionally put here in case more methods are added to StoredFunctions
# in the future.
# class StoredFunction():
#     def init(self):
#         """
#         Initializes an StoredFunction
#         :return:
#         """
#     def delete(self):
#         """
#         Delete the stored function specified in the query string parameter
#         https://stardog-union.github.io/http-docs/#operation/deleteStoredFunctionNamed
#         :return:
#        """


class Cache:
    """Cached data

    A cached dataset from a query or named/virtual graph.

    See Also:
        `Stardog Docs - Cache Management <https://docs.stardog.com/high-availability-cluster/operating-the-cluster/cache-management>`_
    """

    def __init__(self, name, client):
        """Initializes a new cached dataset from a query or named/virtual graph.

        Use :meth:`stardog.admin.Admin.new_cached_graph` or
        :meth:`stardog.admin.Admin.new_cached_query` instead of
        constructing manually.
        """
        self.name = name
        self.client = client
        self.status()  # raises exception if cache doesn't exist on the server

    def drop(self) -> None:
        """Drops the cache."""
        url_encoded_name = urllib.parse.quote_plus(self.name)
        self.client.delete("/admin/cache/{}".format(url_encoded_name))

    def refresh(self) -> None:
        """Refreshes the cache."""
        url_encoded_name = urllib.parse.quote_plus(self.name)
        self.client.post("/admin/cache/refresh/{}".format(url_encoded_name))

    def status(self) -> Dict:
        """Retrieves the status of the cache."""
        r = self.client.post("/admin/cache/status", json=[self.name])
        return r.json()

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name


class CacheTarget:
    """Cache Target Server"""

    def __init__(self, name, client):
        """Initializes a cache target.

        Use :meth:`stardog.admin.Admin.new_cache_target` instead of
        constructing manually.
        """
        self.cache_target_name = name
        self.path = "/admin/cache/target/{}".format(name)
        self.client = client

    @property
    def name(self):
        """The name (URI) of the cache target."""
        return self.cache_target_name

    def info(self):
        """Get info for the cache target

        Returns:
          dict: Info
        """

        return self.__cache_target_info()

    def orphan(self):
        """Orphans the cache target but do not destroy its contents."""
        self.client.delete(self.path + "/orphan")

    def remove(self):
        """Removes the cache target and destroy its contents."""
        self.client.delete(self.path)

    def __wait_for_registering_cache_target(self):
        retries = 0
        while True:
            r = self.client.get("/admin/cache/target")
            cache_target_info = next(
                (t for t in r.json() if t["name"] == self.name), {}
            )
            if not cache_target_info:
                retries += 1
                sleep(1)
                if retries >= 20:
                    raise Exception("Took too long to read cache target: " + self.name)
            else:
                return cache_target_info

    def __cache_target_info(self):
        cache_target_info = self.__wait_for_registering_cache_target()
        return cache_target_info

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name
