"""Administer a Stardog server.

"""

import json
import contextlib2
import urllib
from time import sleep

from . import content_types as content_types
from .http import client


class Admin:
    """Admin Connection.

    This is the entry point for admin-related operations on a Stardog server.

    See Also:
      https://www.stardog.com/docs/#_administering_stardog
    """

    def __init__(
        self,
        endpoint: object = None,
        username: object = None,
        password: object = None,
        auth: object = None,
        run_as: str = None,
    ) -> None:
        """Initializes an admin connection to a Stardog server.

        Args:
          endpoint (str, optional): Url of the server endpoint.
            Defaults to `http://localhost:5820`
          username (str, optional): Username to use in the connection.
            Defaults to `admin`
          password (str, optional): Password to use in the connection.
            Defaults to `admin`
          auth (requests.auth.AuthBase, optional): requests Authentication object.
            Defaults to `None`
          run_as (str, optional): the User to impersonate

        auth and username/password should not be used together.  If the are the value
        of `auth` will take precedent.
        Examples:
          >>> admin = Admin(endpoint='http://localhost:9999',
                            username='admin', password='admin')
        """
        self.client = client.Client(
            endpoint, None, username, password, auth=auth, run_as=run_as
        )
        # ensure the server is alive and at the specified location
        self.alive()

    def shutdown(self):
        """Shuts down the server."""
        self.client.post("/admin/shutdown")

    def alive(self):
        """
        Determine whether the server is running
        :return: Returns True if server is alive
        :rtype: bool
        """
        r = self.client.get("/admin/alive")
        return r.status_code == 200

    def healthcheck(self):
        """
        Determine whether the server is running and able to accept traffic
        :return: Returns true if server is able to accept traffic
        :rtype: bool
        """
        r = self.client.get("/admin/healthcheck")
        return r.status_code == 200

    def get_prometheus_metrics(self):
        """ """
        r = self.client.get("/admin/status/prometheus")
        return r.text

    def get_server_metrics(self):
        """
        Return metric information from the registry in JSON format
        https://stardog-union.github.io/http-docs/#operation/status
        :return: Server metrics
        :rtype: dict
        """
        r = self.client.get("/admin/status")
        return r.json()

    def database(self, name):
        """Retrieves an object representing a database.

        Args:
          name (str): The database name

        Returns:
          Database: The requested database
        """
        return Database(name, self.client)

    def databases(self):
        """Retrieves all databases.

        Returns:
          list[Database]: A list of database objects
        """
        r = self.client.get("/admin/databases")
        databases = r.json()["databases"]
        return list(map(lambda name: Database(name, self.client), databases))

    def new_database(self, name, options=None, *contents, **kwargs):
        """Creates a new database.

        Args:
          name (str): the database name
          options (dict): Dictionary with database options (optional)
          *contents (Content or (Content, str), optional): Datasets
            to perform bulk-load with. Named graphs are made with tuples of
            Content and the name.
          **kwargs: Allows to set copy_to_server. If true, sends the files to the Stardog server,
            and replicates them to the rest of nodes.

        Returns:
            Database: The database object

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

    def restore(self, from_path, *, name=None, force=False):
        """Restore a database.

        Args:
          from_path (str): the full path on the server to the backup
          name (str, optional): the name of the database to
            restore to if different from the backup
          force (boolean, optional): a backup will not be restored over an
            existing database of the same name; the force flag should be used
            to overwrite the database

        Examples:
            >>> admin.restore("/data/stardog/.backup/db/2019-12-01")
            >>> admin.restore("/data/stardog/.backup/db/2019-11-05",
                              name="db2", force=True)

        See Also:
          https://www.stardog.com/docs/#_restore_a_database_from_a_backup
        """
        params = {"from": from_path, "force": force}
        if name:
            params["name"] = name

        self.client.put("/admin/restore", params=params)

    def backup_all(self, location=None):
        """
        Create a backup of all databases on the server
        """
        url = "/admin/databases/backup_all"
        if location is not None:
            params = urllib.parse.urlencode({"to": location})
            url = f"{url}?{params}"
        self.client.put(url)

    def get_all_metadata_properties(self):
        """
        Get information on all database metadata properties, including description and example values
        :return: Metadata properties
        :rtype: dict
        """
        r = self.client.get("/admin/config_properties")
        return r.json()

    def query(self, id):
        """Gets information about a running query.

        Args:
          id (str): Query ID

        Returns:
            dict: Query information
        """
        r = self.client.get("/admin/queries/{}".format(id))
        return r.json()

    def queries(self):
        """Gets information about all running queries.

        Returns:
          dict: Query information
        """
        r = self.client.get("/admin/queries")
        return r.json()["queries"]

    def kill_query(self, id):
        """Kills a running query.

        Args:
          id (str): ID of the query to kill
        """
        self.client.delete("/admin/queries/{}".format(id))

    def stored_query(self, name):
        """Retrieves a Stored Query.

        Args:
          name (str): The name of the Stored Query to retrieve

        Returns:
          StoredQuery: The StoredQuery object
        """
        return StoredQuery(name, self.client)

    def stored_queries(self):
        """Retrieves all stored queries.

        Returns:
          list[StoredQuery]: A list of StoredQuery objects
        """
        r = self.client.get(
            "/admin/queries/stored", headers={"Accept": "application/json"}
        )
        queries = r.json()["queries"]
        return list(
            map(lambda query: StoredQuery(query["name"], self.client, query), queries)
        )

    def new_stored_query(self, name, query, options=None):
        """Creates a new Stored Query.

        Args:
          name (str): The name of the stored query
          query (str): The query text
          options (dict, optional): Additional options

        Returns:
          StoredQuery: the new StoredQuery

        Examples:
            >>> admin.new_stored_query('all triples',
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

    def clear_stored_queries(self):
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

    def user(self, name):
        """Retrieves an object representing a user.

        Args:
          name (str): The name of the user

        Returns:
          User: The User object
        """
        return User(name, self.client)

    def users(self):
        """Retrieves all users.

        Returns:
          list[User]: A list of User objects
        """
        r = self.client.get("/admin/users")
        users = r.json()["users"]
        return list(map(lambda name: User(name, self.client), users))

    def new_user(self, username, password, superuser=False):
        """Creates a new user.

        Args:
          username (str): The username
          password (str): The password
          superuser (bool): Should the user be super? Defaults to false.

        Returns:
          User: The new User object
        """
        meta = {
            "username": username,
            "password": list(password),
            "superuser": superuser,
        }

        self.client.post("/admin/users", json=meta)
        return self.user(username)

    def role(self, name):
        """Retrieves an object representing a role.

        Args:
          name (str): The name of the Role

        Returns:
          Role: The Role object
        """
        return Role(name, self.client)

    def roles(self):
        """Retrieves all roles.

        Returns:
          list[Role]: A list of Role objects
        """
        r = self.client.get("/admin/roles")
        roles = r.json()["roles"]
        return list(map(lambda name: Role(name, self.client), roles))

    def new_role(self, name):
        """Creates a  new role.

        Args:
          name (str): The name of the new Role

        Returns:
          Role: The new Role object
        """
        self.client.post("/admin/roles", json={"rolename": name})
        return Role(name, self.client)

    def virtual_graph(self, name):
        """Retrieves a Virtual Graph.

        Args:
          name (str): The name of the Virtual Graph to retrieve

        Returns:
          VirtualGraph: The VirtualGraph object
        """
        return VirtualGraph(name, self.client)

    def virtual_graphs(self):
        """Retrieves all virtual graphs.

        Returns:
          list[VirtualGraph]: A list of VirtualGraph objects
        """
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

    # deprecated
    def import_virtual_graph(self, db, mappings, named_graph, remove_all, options):
        """Import (materialize) a virtual graph directly into the local knowledge graph.

        Args:
          db (str): The database into which to import the graph
          mappings (MappingFile or MappingRaw): New mapping contents. An empty string can be passed for autogenerated mappings.
          named_graph (str): Name of the graph into which import the VG.
          remove_all (bool): Should the target named graph be cleared before importing?
          options (dict): Options for the new virtual graph, https://docs.stardog.com/virtual-graphs/virtual-graph-configuration#virtual-graph-properties

        Examples:
            >>> admin.import_virtual_graph(
                  'db-name', mappings=File('mappings.ttl'),
                  named_graph='my-graph', remove_all=True, options={'jdbc.driver': 'com.mysql.jdbc.Driver'}
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
        db,
        mappings,
        data_source=None,
        options=None,
        named_graph="tag:stardog:api:context:default",
        remove_all=False,
    ):
        """Import (materialize) a virtual graph directly into the local knowledge graph.

        Args:
          db (str): The database into which to import the graph
          mappings (MappingFile or MappingRaw): New mapping contents. An empty string can be passed for autogenerated mappings.
          data_source (str, optional): The datasource to load from
          options (dict, optional): Options for the new virtual graph, https://docs.stardog.com/virtual-graphs/virtual-graph-configuration#virtual-graph-properties
          named_graph (str,optional): Name of the graph into which import the VG. Default: Default graph
          remove_all (bool, optional): Should the target named graph be cleared before importing? Default: False

        Examples:
            >>> admin.materialize_virtual_graph(
                  'db-name', mappings=File('mappings.ttl'),
                  named_graph='my-graph'
                )
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
                    options["mappings.syntax"] = "STARDOG"
                else:
                    options = {
                        "mappings.syntax": "STARDOG"
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
        self, name, mappings=None, options=None, datasource=None, db=None
    ):
        """Creates a new Virtual Graph.

        Args:
          name (str): The name of the virtual graph.
          mappings (MappingFile or MappingRaw, optional): New mapping contents, if not pass it will autogenerate
          options (dict, Optional): Options for the new virtual graph. If not passed, then a datasource must be specified.
          datasource (str, Optional): Name of the datasource to use. If not passed, options with a datasource must be set.
          db (str, Optional): Name of the database to associate the VG. If not passed, will be associated to all databases.

        Returns:
          VirtualGraph: the new VirtualGraph

        Examples:
            >>> admin.new_virtual_graph(
                  'users', MappingFile('mappings.sms'), None, 'my_datasource'
                )
            >>> admin.new_virtual_graph(
                  'users', MappingFile('mappings.ttl','SMS2'), None, 'my_datasource'
                )
            >>> admin.new_virtual_graph(  #DEPRECATED
                  'users', File('mappings.ttl'),
                  {'jdbc.driver': 'com.mysql.jdbc.Driver'}
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
                    options["mappings.syntax"] = "STARDOG"
                else:
                    options = {
                        "mappings.syntax": "STARDOG"
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

    def import_file(self, db, mappings, input_file, options=None, named_graph=None):
        """Import a JSON or CSV file.

        Args:
          db (str): Name of the database to import the data
          mappings (MappingRaw or MappingFile): New mapping contents.
          input_file(ImportFile or ImportRaw):
          options (dict, Optional): Options for the new csv import.
          named_graph (str, Optional): The namegraph to associate it too

        Returns:
            r.ok

        Examples:
            >>> admin.import_file(
                  'mydb', File('mappings.ttl'),
                  'test.csv'
                )
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

    def datasource(self, name):
        """Retrieves an object representing a DataSource.

        Args:
          name (str): The name of the data source

        Returns:
          DataSource: The DataSource object
        """
        return DataSource(name, self.client)

    def datasources(self):
        """Retrieves all data sources.

        Returns:
          list[DataSources]: A list of DataSources
        """
        r = self.client.get("/admin/data_sources")
        data_sources = r.json()["data_sources"]
        return list(map(lambda name: DataSource(name, self.client), data_sources))

    def datasources_info(self):
        """List data sources info

        Returns:
          list: A list of data sources info
        """

        r = self.client.get("/admin/data_sources/list")
        return r.json()["data_sources"]

    def new_datasource(self, name, options):
        """Creates a new DataSource.

        Args:
          name (str): The name of the data source
          options (dict): Data source options

        Returns:
          User: The new DataSource object
        """

        if options is None:
            options = {}

        meta = {"name": name, "options": options}

        self.client.post("/admin/data_sources", json=meta)
        return DataSource(name, self.client)

    def get_server_properties(self):
        """Get the value of any set server-level properties

        Returns
            dict: Server properties
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

    def validate(self):
        """Validates an admin connection.

        Returns:
          bool: The connection state
        """
        r = self.client.get("/admin/users/valid")
        return r.status_code == 200

    def cluster_list_standby_nodes(self):
        """
        List standby nodes
        :return: Returns the registry ID for the standby nodes.
        :rtype: dict
        """
        r = self.client.get("/admin/cluster/standby/registry")
        return r.json()

    def cluster_join(self):
        """
        Instruct a standby node to join its cluster as a full node
        """
        self.client.put("/admin/cluster/standby/join")

    def standby_node_pause_status(self):
        """
        Get the pause status of a standby node
        https://stardog-union.github.io/http-docs/#operation/getPauseState
        :return: Pause status of a standby node, possible values are: "WAITING", "SYNCING", "PAUSING", "PAUSED"
        :rtype: Dict
        """
        r = self.client.get("/admin/cluster/standby/pause")
        return r.json()

    def standby_node_pause(self, pause):
        """
        Pause/Unpause standby node
        Args:
          *pause: (boolean): True for pause, False for unpause
        :return: Returns True if the pause status was modified successfully, false if it failed.
        :rtype: bool
        """
        if pause:
            r = self.client.put("/admin/cluster/standby/pause?pause=true")
        else:
            r = self.client.put("/admin/cluster/standby/pause?pause=false")
        return r.status_code == 200

    def cluster_revoke_standby_access(self, registry_id):
        """
        Instruct a standby node to stop syncing
        Args:
          *registry_id: (string): Id of the standby node to stop syncing.
        """
        self.client.delete("/admin/cluster/standby/registry/" + registry_id)

    def cluster_start_readonly(self):
        """
        Start read only mode
        """
        self.client.put("/admin/cluster/readonly")

    def cluster_stop_readonly(self):
        """
        Stops read only mode
        """
        self.client.delete("/admin/cluster/readonly")

    def cluster_coordinator_check(self):
        """
        Determine if a specific cluster node is the cluster coordinator
        :return: True if the node is a coordinator, false if not.
        :rtype: Bool
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

    def cluster_status(self):
        """Prints status information for each node
        in the cluster

        :return: Nodes of the cluster and extra information
        :rtype: dict
        """
        r = self.client.get("/admin/cluster/status")
        return r.json()

    def cluster_info(self):
        """Prints info about the nodes in the Stardog
        Pack cluster.

        :return: Nodes of the cluster
        :rtype: dict
        """
        r = self.client.get("/admin/cluster")
        return r.json()

    def cluster_shutdown(self):
        """
        Shutdown all nodes
        :return: True if the cluster got shutdown successfully.
        :rtype: bool
        """
        r = self.client.post("/admin/shutdownAll")
        return r.status_code == 200

    def cache(self, name):
        """Retrieve an object representing a cached dataset.

        Returns:
          Cache: The requested cache
        """
        return Cache(name, self.client)

    def cache_status(self, *names):
        """Retrieves the status of one or more cached graphs or queries.

        Args:
          *names: (str): Names of the cached graphs or queries
        Returns:
          list[str]: List of statuses
        """
        return self.client.post("/admin/cache/status", json=names).json()

    def cached_status(self):
        """Retrieves all cached queries.

        Returns:
          list[Cache]: A list of Cache objects
        """
        r = self.client.get("/admin/cache/status")
        cache_names = [cache_name["name"] for cache_name in r.json()]
        return list(map(lambda name: Cache(name, self.client), cache_names))

    def cached_queries(self):
        """Retrieves all cached queries. This method is deprecated in Stardog 8+

        Returns:
          list[Cache]: A list of Cache objects
        """
        r = self.client.get("/admin/cache/queries")
        cache_names = [cache_name["name"] for cache_name in r.json()]
        return list(map(lambda name: Cache(name, self.client), cache_names))

    def cached_graphs(self):
        """Retrieves all cached graphs.

        Returns:
          list[Cache]: A list of Cache objects
        """
        r = self.client.get("/admin/cache/graphs")
        cache_names = [cache_name["name"] for cache_name in r.json()]
        return list(map(lambda name: Cache(name, self.client), cache_names))

    def new_cached_query(
        self, name, target, query, database, refresh_script=None, register_only=False
    ):
        """Creates a new cached query.

        Args:
          name (str): The name (URI) of the cached query
          target (str): The name (URI) of the cache target
          query (str): The query to cache
          database (str): The name of the database
          refresh_script (str, optional): A SPARQL insert query to run
            when refreshing the cache
          register_only (bool): Default: false. If true, register a
            cached dataset without loading data from the source graph
            or query into the cache target's databases

        Returns:
          Cache: The new Cache
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
        name,
        target,
        graph,
        database=None,
        refresh_script=None,
        register_only=False,
    ):
        """Creates a new cached graph.

        Args:
          name (str): The name (URI) of the cached query
          target (str): The name (URI) of the cache target
          graph (str): The name of the graph to cache
          database (str): The name of the database. Optional for virtual graphs, mandatory for named graphs.
          refresh_script (str): An optional SPARQL Insert query to run when refreshing the cache.
          register_only (bool): An optional value that if true, register a cached dataset without loading data from the source graph or query into the cache target's databases.


        Returns:
          Cache: The new Cache"""

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

    def cache_targets(self):
        """Retrieves all cache targets.

        Returns:
          list[CacheTarget]: A list of CacheTarget objects
        """
        r = self.client.get("/admin/cache/target")
        return list(
            map(lambda target: CacheTarget(target["name"], self.client), r.json())
        )

    def new_cache_target(
        self, name, hostname, port, username, password, use_existing_db=False
    ):
        """Creates a new cache target.

        Args:
          name (str): The name of the cache target
          hostname (str): The hostname of the cache target server
          port (int): The port of the cache target server
          username (int): The username for the cache target
          password (int): The password for the cache target
          use_existing_db (bool): If true, check for an existing cache database to use before creating a new one

        Returns:
          CacheTarget: The new CacheTarget
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
    """Database Admin

    See Also:
      https://www.stardog.com/docs/#_database_admin
    """

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
    def name(self):
        """The name of the database."""
        return self.database_name

    def get_options(self, *options):
        """Get the value of specific metadata options for a database

        Args:
          *options (str): Database option names

        Returns:
          dict: Database options

        Examples
          >>> db.get_options('search.enabled', 'spatial.enabled')
        """
        # transform into {'option': None} dict
        meta = dict([(x, None) for x in options])

        r = self.client.put(self.path + "/options", json=meta)
        return r.json()

    def get_all_options(self):
        """Get the value of every metadata option for a database

        :return: Dict detailing all database metadata
        :rtype: Dict
        """
        r = self.client.get(self.path + "/options")
        return r.json()

    def set_options(self, options):
        """Sets database options.

        The database must be offline.

        Args:
          options (dict): Database options

        Examples
            >>> db.set_options({'spatial.enabled': False})
        """

        r = self.client.post(self.path + "/options", json=options)
        return r.status_code == 200

    def optimize(self):
        """Optimizes a database."""
        self.client.put(self.path + "/optimize")

    def verify(self):
        """verifies a database."""
        self.client.post(self.path + "/verify")

    def repair(self):
        """Attempt to recover a corrupted database.

        The database must be offline.
        """
        r = self.client.post(self.path + "/repair")
        return r.status_code == 200

    def backup(self, *, to=None):
        """Create a backup of a database on the server.

        Args:
          to (string, optional): specify a path on the server to store
            the backup

        See Also:
          https://www.stardog.com/docs/#_backup_a_database
        """
        params = {"to": to} if to else {}
        self.client.put(self.path + "/backup", params=params)

    def online(self):
        """Sets a database to online state."""
        self.client.put(self.path + "/online")

    def offline(self):
        """Sets a database to offline state."""
        self.client.put(self.path + "/offline")

    def copy(self, to):
        """Makes a copy of this database under another name.

        The database must be offline.

        Args:
          to (str): Name of the new database to be created

        Returns:
          Database: The new Database
        """
        self.client.put(self.path + "/copy", params={"to": to})
        return Database(to, self.client)

    def drop(self):
        """Drops the database."""
        self.client.delete(self.path)

    def namespaces(self):
        """
        Retrieve the namespaces stored in the database
        https://stardog-union.github.io/http-docs/#operation/getNamespaces
        :return: A dict listing the prefixes and IRIs of the stored namespaces
        :rtype: Dict
        """
        r = self.client.get(f"/{self.database_name}/namespaces")
        return r.json()["namespaces"]

    def import_namespaces(self, content):
        """
        Args:
          content (Content): RDF File containing prefix declarations
            Imports namespace prefixes from an RDF file
            that contains prefix declarations into the database, overriding any
            previous mappings for those prefixes. Only the prefix declarations in
            the file are processed, the rest of the file is not parsed.

        :return: Dictionary with all namespaces after import
        :rtype: Dict
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

    def add_namespace(self, prefix, iri):
        """Adds a specific namespace to a database
        :return: True if the operation succeeded.
        :rtype: Bool
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

    def remove_namespace(self, prefix):
        """Removes a specific namespace from a database
        :return: True if the operation succeeded.
        :rtype: Bool
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
        https://www.stardog.com/docs/#_list_stored_queries
        https://www.stardog.com/docs/#_managing_stored_queries
    """

    def __init__(self, name, client, details=None):
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
    def name(self):
        """The name of the stored query."""
        return self.query_name

    @property
    def description(self):
        """The description of the stored query."""
        return self.details["description"]

    @property
    def creator(self):
        """The creator of the stored query."""
        return self.details["creator"]

    @property
    def database(self):
        """The database the stored query applies to."""
        return self.details["database"]

    @property
    def query(self):
        """The text of the stored query."""
        return self.details["query"]

    @property
    def shared(self):
        """The value of the shared property."""
        return self.details["shared"]

    @property
    def reasoning(self):
        """The value of the reasoning property."""
        return self.details["reasoning"]

    def update(self, **options):
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

    def delete(self):
        """Deletes the Stored Query."""
        self.client.delete(self.path)

    def __eq__(self, other):
        return self.name == other.name


class User:
    """User

    See Also:
      https://www.stardog.com/docs/#_security
    """

    def __init__(self, name, client):
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
    def name(self):
        """str: The user name."""
        return self.username

    def set_password(self, password):
        """Sets a new password.

        Args:
          password (str)
        """
        self.client.put(self.path + "/pwd", json={"password": password})

    def is_enabled(self):
        """Checks if the user is enabled.

        Returns:
          bool: User activation state
        """
        r = self.client.get(self.path + "/enabled")
        return bool(r.json()["enabled"])

    def set_enabled(self, enabled):
        """Enables or disables the user.

        Args:
          enabled (bool): Desired User state
        """
        self.client.put(self.path + "/enabled", json={"enabled": enabled})

    def is_superuser(self):
        """Checks if the user is a super user.

        Returns:
          bool: Superuser state
        """
        r = self.client.get(self.path + "/superuser")
        return bool(r.json()["superuser"])

    def roles(self):
        """Gets all the User's roles.

        Returns:
          list[Role]
        """
        r = self.client.get(self.path + "/roles")
        roles = r.json()["roles"]
        return list(map(lambda name: Role(name, self.client), roles))

    def add_role(self, role):
        """Adds an existing role to the user.

        Args:
          role (str or Role): The role to add or its name

        Examples:
            >>> user.add_role('reader')
            >>> user.add_role(admin.role('reader'))
        """
        self.client.post(self.path + "/roles", json={"rolename": role})

    def set_roles(self, *roles):
        """Sets the roles of the user.

        Args:
          *roles (str or Role): The roles to add the User to

        Examples
            >>> user.set_roles('reader', admin.role('writer'))
        """
        roles = list(map(self.__rolename, roles))
        self.client.put(self.path + "/roles", json={"roles": roles})

    def remove_role(self, role):
        """Removes a role from the user.

        Args:
          role (str or Role): The role to remove or its name

        Examples
            >>> user.remove_role('reader')
            >>> user.remove_role(admin.role('reader'))
        """
        self.client.delete(self.path + "/roles/" + role)

    def delete(self):
        """Deletes the user."""
        self.client.delete(self.path)

    def permissions(self):
        """Gets the user permissions.

        See Also:
          https://www.stardog.com/docs/#_permissions

        Returns:
          dict: User permissions
        """
        r = self.client.get("/admin/permissions/user/{}".format(self.name))
        return r.json()["permissions"]

    def add_permission(self, action, resource_type, resource):
        """Add a permission to the user.

        See Also:
          https://www.stardog.com/docs/#_permissions

        Args:
          action (str): Action type (e.g., 'read', 'write')
          resource_type (str): Resource type (e.g., 'user', 'db')
          resource (str): Target resource (e.g., 'username', '*')

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

    def remove_permission(self, action, resource_type, resource):
        """Removes a permission from the user.

        See Also:
          https://www.stardog.com/docs/#_permissions

        Args:
          action (str): Action type (e.g., 'read', 'write')
          resource_type (str): Resource type (e.g., 'user', 'db')
          resource (str): Target resource (e.g., 'username', '*')

        Examples
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

    def effective_permissions(self):
        """Gets the user's effective permissions.

        Returns:
          dict: User effective permissions
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
        https://www.stardog.com/docs/#_security
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
            https://www.stardog.com/docs/#_permissions

        Returns:
          dict: Role permissions
        """
        r = self.client.get("/admin/permissions/role/{}".format(self.name))
        return r.json()["permissions"]

    def add_permission(self, action, resource_type, resource):
        """Adds a permission to the role.

        See Also:
            https://www.stardog.com/docs/#_permissions

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
            https://www.stardog.com/docs/#_permissions

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
        https://www.stardog.com/docs/#_structured_data
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
    def name(self):
        """The name of the virtual graph."""
        return self.graph_name

    def update(self, name, mappings, options={}, datasource=None, db=None):
        """Updates the Virtual Graph.

        Args:
          name (str): The new name
          mappings (Content): New mapping contents
          options (dict): New options

        Examples:
            >>> vg.update('users', File('mappings.ttl'),
                         {'jdbc.driver': 'com.mysql.jdbc.Driver'})
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

    def delete(self):
        """Deletes the Virtual Graph."""
        self.client.delete(self.path)

    def options(self):
        """Gets Virtual Graph options.

        Returns:
          dict: Options
        """
        r = self.client.get(self.path + "/options")
        return r.json()["options"]

    def info(self):
        """Gets Virtual Graph info.

        Returns:
          dict: Info
        """

        r = self.client.get(self.path + "/info")
        return r.json()["info"]

    # should return object or name?
    def get_datasource(self):
        """Gets datasource associated with the VG

        :return: datasource name
        """
        return self.info()["data_source"].replace("data-source://", "")

    # should return object or name?
    def get_database(self):
        """Gets database associated with the VirtualGraph.

        Returns:
          string: Database name
        """

        r = self.client.get(self.path + "/database")
        return r.text

    def mappings_string(self, syntax="STARDOG"):
        """Returns graph mappings as RDF
        Args:
          syntax (str): The desired RDF syntax of the mappings (STARDOG, R2RML, or SMS2).
          Defaults to 'STARDOG'

        :return: Mappings in given content type
        :rtype: string
        """
        r = self.client.get(f"{self.path}/mappingsString/{syntax}")
        return r.content

    # Should test this. The docs state the path is /mappingsString, but this one goes to /mappings.
    # Either the docs, or this implementation is wrong.
    def mappings(self, content_type=content_types.TURTLE):
        """Gets the Virtual Graph mappings (Deprecated, see mappings_string instead).

        Args:
          content_type (str): Content type for results.
          Defaults to 'text/turtle'

        :return: Mappings in given content type
        :rtype: bytes
        """
        r = self.client.get(self.path + "/mappings", headers={"Accept": content_type})
        return r.content

    def available(self):
        """Checks if the Virtual Graph is available.

        Returns:
          bool: Availability state
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
        https://docs.stardog.com/virtual-graphs/virtual-sources
    """

    def __init__(self, name, client):
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
    def name(self):
        """The name of the data source."""
        return self.data_source_name

    def available(self):
        """Checks if the data source is available.

        Returns:
          bool: Availability state
        """
        r = self.client.get(self.path + "/available")
        return bool(r.json())

    def refresh_count(self, meta=None):
        """Refresh table row-count estimates"""

        if meta is None:
            meta = {}

        self.client.post(self.path + "/refresh_counts", json=meta)

    def update(self, options=None, force=False):
        """Update data source

        Args:
            options (dict): Dictionary with data source options (optional)
            force (boolean, optional): a data source will not be updated while
            in use unless the force flag is set to True

        Examples:
            >>> admin.update({"sql.dialect": "MYSQL"})
            >>> admin.update({"sql.dialect": "MYSQL"}, force=True)

        See Also:
            https://docs.stardog.com/virtual-graphs/virtual-graph-configuration#virtual-graph-options
        """
        if options is None:
            options = {}

        meta = {"options": options, "force": force}

        self.client.put(self.path, json=meta)

    def delete(self):
        """Deletes a data source"""

        self.client.delete(self.path)

    def online(self):
        """Online a data source"""

        self.client.post(self.path + "/online")

    def info(self):
        """Get data source info

        Returns:
          dict: Info
        """

        r = self.client.get(self.path + "/info")
        return r.json()["info"]

    def refresh_metadata(self, meta=None):
        """Refresh metadata"""

        if meta is None:
            meta = {}

        self.client.post(self.path + "/refresh_metadata", json=meta)

    def share(self):
        """Share data source"""

        self.client.post(self.path + "/share")

    def get_options(self):
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
        https://www.stardog.com/docs/#_cache_management
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

    def drop(self):
        """Drops the cache."""
        url_encoded_name = urllib.parse.quote_plus(self.name)
        self.client.delete("/admin/cache/{}".format(url_encoded_name))

    def refresh(self):
        """Refreshes the cache."""
        url_encoded_name = urllib.parse.quote_plus(self.name)
        self.client.post("/admin/cache/refresh/{}".format(url_encoded_name))

    def status(self):
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
