"""Administer a Stardog server.
"""

import json
import contextlib2

from . import content_types as content_types
from .http import client


class Admin(object):
    """Admin Connection.

    This is the entry point for admin-related operations on a Stardog server.

    See Also:
      https://www.stardog.com/docs/#_administering_stardog
    """

    def __init__(self, endpoint=None, username=None, password=None, auth=None):
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

        auth and username/password should not be used together.  If the are the value
        of `auth` will take precedent.
        Examples:
          >>> admin = Admin(endpoint='http://localhost:9999',
                            username='admin', password='admin')
        """
        self.client = client.Client(endpoint, None, username, password, auth=auth)

    def shutdown(self):
        """Shuts down the server.
        """
        self.client.post('/admin/shutdown')

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
        r = self.client.get('/admin/databases')
        databases = r.json()['databases']
        return list(map(lambda name: Database(name, self.client), databases))

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
        fmetas = []
        params = []

        with contextlib2.ExitStack() as stack:
            for c in contents:
                content = c[0] if isinstance(c, tuple) else c
                context = c[1] if isinstance(c, tuple) else None

                # we will be opening references to many sources in a
                # single call use a stack manager to make sure they
                # all get properly closed at the end
                data = stack.enter_context(content.data())
                fname = content.name
                fmeta = {'filename': fname}

                if context:
                    fmeta['context'] = context

                fmetas.append(fmeta)
                params.append((fname, (fname, data, content.content_type, {
                    'Content-Encoding': content.content_encoding
                })))

            meta = {
                'dbname': name,
                'options': options if options else {},
                'files': fmetas
            }

            params.append(('root', (None, json.dumps(meta), 'application/json')))
            self.client.post('/admin/databases', files=params)

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
        params = {
            'from': from_path,
            'force': force
        }
        if name:
            params['name'] = name

        self.client.put('/admin/restore', params=params)

    def query(self, id):
        """Gets information about a running query.

        Args:
          id (str): Query ID

        Returns:
            dict: Query information
        """
        r = self.client.get('/admin/queries/{}'.format(id))
        return r.json()

    def queries(self):
        """Gets information about all running queries.

        Returns:
          dict: Query information
        """
        r = self.client.get('/admin/queries')
        return r.json()['queries']

    def kill_query(self, id):
        """Kills a running query.

        Args:
          id (str): ID of the query to kill
        """
        self.client.delete('/admin/queries/{}'.format(id))

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
        r = self.client.get('/admin/queries/stored',
                            headers={'Accept': 'application/json'})
        query_names = [q['name'] for q in r.json()['queries']]
        return list(map(lambda name: StoredQuery(name, self.client), query_names))

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

        meta = {
            'name': name,
            'query': query,
            'creator': self.client.username
        }
        meta.update(options)

        self.client.post('/admin/queries/stored', json=meta)
        return StoredQuery(name, self.client)

    def clear_stored_queries(self):
        """Remove all stored queries on the server.
        """
        self.client.delete('/admin/queries/stored')

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
        r = self.client.get('/admin/users')
        users = r.json()['users']
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
            'username': username,
            'password': list(password),
            'superuser': superuser,
        }

        self.client.post('/admin/users', json=meta)
        return self.user(username)
        return User(username, self.client)

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
        r = self.client.get('/admin/roles')
        roles = r.json()['roles']
        return list(map(lambda name: Role(name, self.client), roles))

    def new_role(self, name):
        """Creates a  new role.

        Args:
          name (str): The name of the new Role

        Returns:
          Role: The new Role object
        """
        self.client.post('/admin/roles', json={'rolename': name})
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
        r = self.client.get('/admin/virtual_graphs')
        virtual_graphs = r.json()['virtual_graphs']
        return list(map(lambda name: VirtualGraph(name, self.client), virtual_graphs))

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
            meta = {
                'name': name,
                'mappings': data.read().decode() if hasattr(data, 'read') else data,
                'options': options,
            }

            self.client.post('/admin/virtual_graphs', json=meta)
            return VirtualGraph(name, self.client)

    def validate(self):
        """Validates an admin connection.

        Returns:
          bool: The connection state
        """
        self.client.get('/admin/users/valid')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()


class Database(object):
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
        self.path = '/admin/databases/{}'.format(name)

    @property
    def name(self):
        """The name of the database.
        """
        return self.database_name

    def get_options(self, *options):
        """Gets database options.

        Args:
          *options (str): Database option names

        Returns:
          dict: Database options

        Examples
          >>> db.get_options('search.enabled', 'spatial.enabled')
        """
        # transform into {'option': None} dict
        meta = dict([(x, None) for x in options])

        r = self.client.put(self.path + '/options', json=meta)
        return r.json()

    def set_options(self, options):
        """Sets database options.

        The database must be offline.

        Args:
          options (dict): Database options

        Examples
            >>> db.set_options({'spatial.enabled': False})
        """
        self.client.post(self.path + '/options', json=options)

    def optimize(self):
        """Optimizes a database.
        """
        self.client.put(self.path + '/optimize')

    def repair(self):
        """Repairs a database.

        The database must be offline.
        """
        self.client.post(self.path + '/repair')

    def backup(self, *, to=None):
        """Backup a database.

        Args:
          to (string, optional): specify a path on the server to store
            the backup

        See Also:
          https://www.stardog.com/docs/#_backup_a_database
        """
        params = {'to': to} if to else {}
        self.client.put(self.path + '/backup', params=params)

    def online(self):
        """Sets a database to online state.
        """
        self.client.put(self.path + '/online')

    def offline(self):
        """Sets a database to offline state.
        """
        self.client.put(self.path + '/offline')

    def copy(self, to):
        """Makes a copy of this database under another name.

        The database must be offline.

        Args:
          to (str): Name of the new database to be created

        Returns:
          Database: The new Database
        """
        self.client.put(self.path + '/copy', params={'to': to})
        return Database(to, self.client)

    def drop(self):
        """Drops the database.
        """
        self.client.delete(self.path)

    def __repr__(self):
        return self.name


class StoredQuery(object):
    """Stored Query

    See Also:
        https://www.stardog.com/docs/#_list_stored_queries
        https://www.stardog.com/docs/#_managing_stored_queries
    """

    def __init__(self, name, client):
        """Initializes a stored query.

        Use :meth:`stardog.admin.Admin.stored_query`,
        :meth:`stardog.admin.Admin.stored_queries`, or
        :meth:`stardog.admin.Admin.new_stored_query` instead of
        constructing manually.
        """
        self.query_name = name
        self.client = client
        self.path = '/admin/queries/stored/{}'.format(name)
        self.details = {}
        self.__refresh()

    def __refresh(self):
        details = self.client.get(self.path, headers={'Accept': 'application/json'})
        self.details.update(details.json()['queries'][0])

    @property
    def name(self):
        """The name of the stored query.
        """
        return self.query_name

    @property
    def description(self):
        """The description of the stored query.
        """
        return self.details['description']

    @property
    def creator(self):
        """The creator of the stored query.
        """
        return self.details['creator']

    @property
    def database(self):
        """The database the stored query applies to.
        """
        return self.details['database']

    @property
    def query(self):
        """The text of the stored query.
        """
        return self.details['query']

    @property
    def shared(self):
        """The value of the shared property.
        """
        return self.details['shared']

    @property
    def reasoning(self):
        """The value of the reasoning property.
        """
        return self.details['reasoning']

    def update(self, **options):
        """Updates the Stored Query.

        Args:
          **options (str): Named arguments to update.

        Examples:
            Update description

            >>> stored_query.update(description='this query finds all the relevant...')
        """
        options['name'] = self.query_name
        for opt in ['query', 'creator']:
            if opt not in options:
                options[opt] = self.__getattribute__(opt)

        self.client.put('/admin/queries/stored', json=options)
        self.__refresh()

    def delete(self):
        """Deletes the Stored Query.
        """
        self.client.delete(self.path)


class User(object):
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
        self.path = '/admin/users/{}'.format(name)

    @property
    def name(self):
        """str: The user name.
        """
        return self.username

    def set_password(self, password):
        """Sets a new password.

        Args:
          password (str)
        """
        self.client.put(self.path + '/pwd', json={'password': password})

    def is_enabled(self):
        """Checks if the user is enabled.

        Returns:
          bool: User activation state
        """
        r = self.client.get(self.path + '/enabled')
        return bool(r.json()['enabled'])

    def set_enabled(self, enabled):
        """Enables or disables the user.

        Args:
          enabled (bool): Desired User state
        """
        self.client.put(self.path + '/enabled', json={'enabled': enabled})

    def is_superuser(self):
        """Checks if the user is a super user.

        Returns:
          bool: Superuser state
        """
        r = self.client.get(self.path + '/superuser')
        return bool(r.json()['superuser'])

    def roles(self):
        """Gets all the User's roles.

        Returns:
          list[Role]
        """
        r = self.client.get(self.path + '/roles')
        roles = r.json()['roles']
        return list(map(lambda name: Role(name, self.client), roles))

    def add_role(self, role):
        """Adds an existing role to the user.

        Args:
          role (str or Role): The role to add or its name

        Examples:
            >>> user.add_role('reader')
            >>> user.add_role(admin.role('reader'))
        """
        self.client.post(self.path + '/roles', json={'rolename': role})

    def set_roles(self, *roles):
        """Sets the roles of the user.

        Args:
          *roles (str or Role): The roles to add the User to

        Examples
            >>> user.set_roles('reader', admin.role('writer'))
        """
        roles = list(map(self.__rolename, roles))
        self.client.put(self.path + '/roles', json={'roles': roles})

    def remove_role(self, role):
        """Removes a role from the user.

        Args:
          role (str or Role): The role to remove or its name

        Examples
            >>> user.remove_role('reader')
            >>> user.remove_role(admin.role('reader'))
        """
        self.client.delete(self.path + '/roles/' + role)

    def delete(self):
        """Deletes the user.
        """
        self.client.delete(self.path)

    def permissions(self):
        """Gets the user permissions.

        See Also:
          https://www.stardog.com/docs/#_permissions

        Returns:
          dict: User permissions
        """
        r = self.client.get('/admin/permissions/user/{}'.format(self.name))
        return r.json()['permissions']

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
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }
        self.client.put(
            '/admin/permissions/user/{}'.format(self.name), json=meta)

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
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }

        self.client.post(
            '/admin/permissions/user/{}/delete'.format(self.name), json=meta)

    def effective_permissions(self):
        """Gets the user's effective permissions.

        Returns:
          dict: User effective permissions
        """
        r = self.client.get('/admin/permissions/effective/user/' + self.name)
        return r.json()['permissions']

    def __rolename(self, role):
        return role.name if isinstance(role, Role) else role


class Role(object):
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
        self.path = '/admin/roles/{}'.format(name)

    @property
    def name(self):
        """The name of the Role.
        """
        return self.role_name

    def users(self):
        """Lists the users for this role.

        Returns:
          list[User]
        """
        r = self.client.get(self.path + '/users')
        users = r.json()['users']
        return list(map(lambda name: User(name, self.client), users))

    def delete(self, force=None):
        """Deletes the role.

        Args:
          force (bool): Force deletion of the role
        """
        self.client.delete(self.path, params={'force': force})

    def permissions(self):
        """Gets the role permissions.

        See Also:
            https://www.stardog.com/docs/#_permissions

        Returns:
          dict: Role permissions
        """
        r = self.client.get('/admin/permissions/role/{}'.format(self.name))
        return r.json()['permissions']

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
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }

        self.client.put(
            '/admin/permissions/role/{}'.format(self.name), json=meta)

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
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }

        self.client.post(
            '/admin/permissions/role/{}/delete'.format(self.name), json=meta)

    def __repr__(self):
        return self.name


class VirtualGraph(object):
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
        self.path = '/admin/virtual_graphs/{}'.format(name)
        self.client = client

    @property
    def name(self):
        """The name of the virtual graph.
        """
        return self.graph_name

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
            meta = {
                'name': name,
                'mappings': data.read().decode() if hasattr(data, 'read') else data,
                'options': options,
            }

            self.client.put(self.path, json=meta)
            self.graph_name = name

    def delete(self):
        """Deletes the Virtual Graph.
        """
        self.client.delete(self.path)

    def options(self):
        """Gets Virtual Graph options.

        Returns:
          dict: Options
        """
        r = self.client.get(self.path + '/options')
        return r.json()['options']

    def mappings(self, content_type=content_types.TURTLE):
        """Gets the Virtual Graph mappings.

        Args:
          content_type (str): Content type for results.
            Defaults to 'text/turtle'

        Returns:
          str: Mappings in given content type
        """
        r = self.client.get(
            self.path + '/mappings', headers={'Accept': content_type})
        return r.content

    def available(self):
        """Checks if the Virtual Graph is available.

        Returns:
          bool: Availability state
        """
        r = self.client.get(self.path + '/available')
        return bool(r.json()['available'])

    def __repr__(self):
        return self.name
