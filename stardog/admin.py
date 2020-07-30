"""Administer a Stardog server.
"""

import contextlib2

from . import content_types as content_types
from .http import admin as http_admin


class Admin(object):
    """Admin Connection.

    This is the entry point for admin-related operations on a Stardog server.

    See Also:
      https://www.stardog.com/docs/#_administering_stardog
    """

    def __init__(self, endpoint=None, username=None, password=None):
        """Initializes an admin connection to a Stardog server.

        Args:
          endpoint (str, optional): Url of the server endpoint.
            Defaults to `http://localhost:5820`
          username (str, optional): Username to use in the connection.
            Defaults to `admin`
          password (str, optional): Password to use in the connection.
            Defaults to `admin`

        Examples:
          >>> admin = Admin(endpoint='http://localhost:9999',
                            username='admin', password='admin')
        """
        self.admin = http_admin.Admin(endpoint, username, password)

    def shutdown(self):
        """Shuts down the server.
        """
        self.admin.shutdown()

    def database(self, name):
        """Retrieves an object representing a database.

        Args:
          name (str): The database name

        Returns:
          Database: The requested database
        """
        return Database(self.admin.database(name))

    def databases(self):
        """Retrieves all databases.

        Returns:
          list[Database]: A list of database objects
        """
        return list(map(Database, self.admin.databases()))

    def new_database(self, name, options=None, *contents):
        """Creates a new database.

        Args:
          name (str): the database name
          options (dict): Dictionary with database options (optional)
          *contents (Content or (Content, str), optional): Datasets
            to perform bulk-load with. Named graphs are made with tuples of
            Content and the name.

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
        files = []

        with contextlib2.ExitStack() as stack:
            for c in contents:
                content = c[0] if isinstance(c, tuple) else c
                context = c[1] if isinstance(c, tuple) else None

                # we will be opening references to many sources in a
                # single call use a stack manager to make sure they
                # all get properly closed at the end
                data = stack.enter_context(content.data())

                files.append({
                    'name': content.name,
                    'content': data,
                    'content-type': content.content_type,
                    'content-encoding': content.content_encoding,
                    'context': context
                })

            return Database(self.admin.new_database(name, options, *files))

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
        self.admin.restore(from_path, name=name, force=force)

    def query(self, id):
        """Gets information about a running query.

        Args:
          id (str): Query ID

        Returns:
            dict: Query information
        """
        return self.admin.query(id)

    def queries(self):
        """Gets information about all running queries.

        Returns:
          dict: Query information
        """
        return self.admin.queries()

    def kill_query(self, id):
        """Kills a running query.

        Args:
          id (str): ID of the query to kill
        """
        self.admin.kill_query(id)

    def stored_query(self, name):
        """Retrieves a Stored Query.

        Args:
          name (str): The name of the Stored Query to retrieve

        Returns:
          StoredQuery: The StoredQuery object
        """
        return StoredQuery(self.admin.stored_query(name))

    def stored_queries(self):
        """Retrieves all stored queries.

        Returns:
          list[StoredQuery]: A list of StoredQuery objects
        """
        return list(map(StoredQuery, self.admin.stored_queries()))

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
        return StoredQuery(self.admin.new_stored_query(name, query, options))

    def clear_stored_queries(self):
        """Remove all stored queries on the server.
        """
        self.admin.clear_stored_queries()

    def user(self, name):
        """Retrieves an object representing a user.

        Args:
          name (str): The name of the user

        Returns:
          User: The User object
        """
        return User(self.admin.user(name))

    def users(self):
        """Retrieves all users.

        Returns:
          list[User]: A list of User objects
        """
        return list(map(User, self.admin.users()))

    def new_user(self, username, password, superuser=False):
        """Creates a new user.

        Args:
          username (str): The username
          password (str): The password
          superuser (bool): Should the user be super? Defaults to false.

        Returns:
          User: The new User object
        """
        return User(self.admin.new_user(username, password, superuser))

    def role(self, name):
        """Retrieves an object representing a role.

        Args:
          name (str): The name of the Role

        Returns:
          Role: The Role object
        """
        return Role(self.admin.role(name))

    def roles(self):
        """Retrieves all roles.

        Returns:
          list[Role]: A list of Role objects
        """
        return list(map(Role, self.admin.roles()))

    def new_role(self, name):
        """Creates a  new role.

        Args:
          name (str): The name of the new Role

        Returns:
          Role: The new Role object
        """
        return Role(self.admin.new_role(name))

    def virtual_graph(self, name):
        """Retrieves a Virtual Graph.

        Args:
          name (str): The name of the Virtual Graph to retrieve

        Returns:
          VirtualGraph: The VirtualGraph object
        """
        return VirtualGraph(self.admin.virtual_graph(name))

    def virtual_graphs(self):
        """Retrieves all virtual graphs.

        Returns:
          list[VirtualGraph]: A list of VirtualGraph objects
        """
        return list(map(VirtualGraph, self.admin.virtual_graphs()))

    def new_virtual_graph(self, name, mappings, options):
        """Creates a new Virtual Graph.

        Args:
          name (str): The name of the virtual graph
          mappings (Content): New mapping contents
          options (dict): Options for the new virtual graph

        Returns:
          VirtualGraph: the new VirtualGraph

        Examples:
            >>> admin.new_virtual_graph(
                  'users', File('mappings.ttl'),
                  {'jdbc.driver': 'com.mysql.jdbc.Driver'}
                )
        """
        with mappings.data() as data:
            return VirtualGraph(
                self.admin.new_virtual_graph(
                    name,
                    data.read().decode() if hasattr(data, 'read') else data,
                    options))

    def validate(self):
        """Validates an admin connection.

        Returns:
          bool: The connection state
        """
        return self.admin.validate()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.admin.__exit__(*args)


class Database(object):
    """Database Admin

    See Also:
      https://www.stardog.com/docs/#_database_admin
    """

    def __init__(self, db):
        """Initializes a Database.

        Use :meth:`stardog.admin.Admin.database`,
        :meth:`stardog.admin.Admin.databases`, or
        :meth:`stardog.admin.Admin.new_database` instead of
        constructing manually.
        """
        self.db = db

    @property
    def name(self):
        """The name of the database.
        """
        return self.db.name

    def get_options(self, *options):
        """Gets database options.

        Args:
          *options (str): Database option names

        Returns:
          dict: Database options

        Examples
          >>> db.get_options('search.enabled', 'spatial.enabled')
        """
        return self.db.get_options(*options)

    def set_options(self, options):
        """Sets database options.

        The database must be offline.

        Args:
          options (dict): Database options

        Examples
            >>> db.set_options({'spatial.enabled': False})
        """
        self.db.set_options(options)

    def optimize(self):
        """Optimizes a database.
        """
        self.db.optimize()

    def repair(self):
        """Repairs a database.

        The database must be offline.
        """
        self.db.repair()

    def backup(self, *, to=None):
        """Backup a database.

        Args:
          to (string, optional): specify a path on the server to store
            the backup

        See Also:
          https://www.stardog.com/docs/#_backup_a_database
        """
        self.db.backup(to=to)

    def online(self):
        """Sets a database to online state.
        """
        self.db.online()

    def offline(self):
        """Sets a database to offline state.
        """
        self.db.offline()

    def copy(self, to):
        """Makes a copy of this database under another name.

        The database must be offline.

        Args:
          to (str): Name of the new database to be created

        Returns:
          Database: The new Database
        """
        db = self.db.copy(to)
        return Database(db)

    def drop(self):
        """Drops the database.
        """
        self.db.drop()


class StoredQuery(object):
    """Stored Query

    See Also:
        https://www.stardog.com/docs/#_list_stored_queries
        https://www.stardog.com/docs/#_managing_stored_queries
    """

    def __init__(self, stored_query):
        """Initializes a stored query.

        Use :meth:`stardog.admin.Admin.stored_query`,
        :meth:`stardog.admin.Admin.stored_queries`, or
        :meth:`stardog.admin.Admin.new_stored_query` instead of
        constructing manually.
        """

        self.stored_query = stored_query

    @property
    def name(self):
        """The name of the stored query.
        """
        return self.stored_query.name

    @property
    def description(self):
        """The description of the stored query.
        """
        return self.stored_query.description

    @property
    def creator(self):
        """The creator of the stored query.
        """
        return self.stored_query.creator

    @property
    def database(self):
        """The database the stored query applies to.
        """
        return self.stored_query.database

    @property
    def query(self):
        """The text of the stored query.
        """
        return self.stored_query.query

    @property
    def shared(self):
        """The value of the shared property.
        """
        return self.stored_query.shared

    @property
    def reasoning(self):
        """The value of the reasoning property.
        """
        return self.stored_query.reasoning

    def update(self, **options):
        """Updates the Stored Query.

        Args:
          **options (str): Named arguments to update.

        Examples:
            Update description

            >>> stored_query.update(description='this query finds all the relevant...')
        """
        self.stored_query.update(**options)

    def delete(self):
        """Deletes the Stored Query.
        """
        self.stored_query.delete()


class User(object):
    """User

    See Also:
      https://www.stardog.com/docs/#_security
    """

    def __init__(self, user):
        """Initializes a User.

        Use :meth:`stardog.admin.Admin.user`,
        :meth:`stardog.admin.Admin.users`, or
        :meth:`stardog.admin.Admin.new_user` instead of
        constructing manually.
        """
        self.user = user

    @property
    def name(self):
        """str: The user name.
        """
        return self.user.name

    def set_password(self, password):
        """Sets a new password.

        Args:
          password (str)
        """
        self.user.set_password(password)

    def is_enabled(self):
        """Checks if the user is enabled.

        Returns:
          bool: User activation state
        """
        return self.user.is_enabled()

    def set_enabled(self, enabled):
        """Enables or disables the user.

        Args:
          enabled (bool): Desired User state
        """
        self.user.set_enabled(enabled)

    def is_superuser(self):
        """Checks if the user is a super user.

        Returns:
          bool: Superuser state
        """
        return self.user.is_superuser()

    def roles(self):
        """Gets all the User's roles.

        Returns:
          list[Role]
        """
        return list(map(Role, self.user.roles()))

    def add_role(self, role):
        """Adds an existing role to the user.

        Args:
          role (str or Role): The role to add or its name

        Examples:
            >>> user.add_role('reader')
            >>> user.add_role(admin.role('reader'))
        """
        self.user.add_role(self.__rolename(role))

    def set_roles(self, *roles):
        """Sets the roles of the user.

        Args:
          *roles (str or Role): The roles to add the User to

        Examples
            >>> user.set_roles('reader', admin.role('writer'))
        """
        self.user.set_roles(*list(map(self.__rolename, roles)))

    def remove_role(self, role):
        """Removes a role from the user.

        Args:
          role (str or Role): The role to remove or its name

        Examples
            >>> user.remove_role('reader')
            >>> user.remove_role(admin.role('reader'))
        """
        self.user.remove_role(self.__rolename(role))

    def delete(self):
        """Deletes the user.
        """
        self.user.delete()

    def permissions(self):
        """Gets the user permissions.

        See Also:
          https://www.stardog.com/docs/#_permissions

        Returns:
          dict: User permissions
        """
        return self.user.permissions()

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
        self.user.add_permission(action, resource_type, resource)

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
        self.user.remove_permission(action, resource_type, resource)

    def effective_permissions(self):
        """Gets the user's effective permissions.

        Returns:
          dict: User effective permissions
        """
        return self.user.effective_permissions()

    def __rolename(self, role):
        return role.name if isinstance(role, Role) else role


class Role(object):
    """Role

    See Also:
        https://www.stardog.com/docs/#_security
    """

    def __init__(self, role):
        """Initializes a Role.

        Use :meth:`stardog.admin.Admin.role`,
        :meth:`stardog.admin.Admin.roles`, or
        :meth:`stardog.admin.Admin.new_role` instead of
        constructing manually.
        """
        self.role = role

    @property
    def name(self):
        """The name of the Role.
        """
        return self.role.name

    def users(self):
        """Lists the users for this role.

        Returns:
          list[User]
        """
        return list(map(User, self.role.users()))

    def delete(self, force=None):
        """Deletes the role.

        Args:
          force (bool): Force deletion of the role
        """
        self.role.delete(force)

    def permissions(self):
        """Gets the role permissions.

        See Also:
            https://www.stardog.com/docs/#_permissions

        Returns:
          dict: Role permissions
        """
        return self.role.permissions()

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
        self.role.add_permission(action, resource_type, resource)

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
        self.role.remove_permission(action, resource_type, resource)


class VirtualGraph(object):
    """Virtual Graph

    See Also:
        https://www.stardog.com/docs/#_structured_data
    """

    def __init__(self, vg):
        """Initializes a virtual graph.

        Use :meth:`stardog.admin.Admin.virtual_graph`,
        :meth:`stardog.admin.Admin.virtual_graphs`, or
        :meth:`stardog.admin.Admin.new_virtual_graph` instead of
        constructing manually.
        """

        self.vg = vg

    @property
    def name(self):
        """The name of the virtual graph.
        """
        return self.vg.name

    def update(self, name, mappings, options):
        """Updates the Virtual Graph.

        Args:
          name (str): The new name
          mappings (Content): New mapping contents
          options (dict): New options

        Examples:
            >>> vg.update('users', File('mappings.ttl'),
                         {'jdbc.driver': 'com.mysql.jdbc.Driver'})
        """
        with mappings.data() as data:
            self.vg.update(
                name,
                data.read().decode() if hasattr(data, 'read') else data,
                options)

    def delete(self):
        """Deletes the Virtual Graph.
        """
        self.vg.delete()

    def options(self):
        """Gets Virtual Graph options.

        Returns:
          dict: Options
        """
        return self.vg.options()

    def mappings(self, content_type=content_types.TURTLE):
        """Gets the Virtual Graph mappings.

        Args:
          content_type (str): Content type for results.
            Defaults to 'text/turtle'

        Returns:
          str: Mappings in given content type
        """
        return self.vg.mappings(content_type)

    def available(self):
        """Checks if the Virtual Graph is available.

        Returns:
          bool: Availability state
        """
        return self.vg.available()
