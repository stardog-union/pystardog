from contextlib2 import ExitStack

from stardog.content_types import TURTLE
from stardog.http.admin import Admin as HTTPAdmin


class Admin(object):
    """
    Admin Connection.
    This is the entry point for admin-related operations on a Stardog server

    See Also
        https://www.stardog.com/docs/#_administering_stardog
    """

    def __init__(self, endpoint=None, username=None, password=None):
        """
        Initialize an admin connection to a Stardog server

        Parameters
            endpoint (str)
                Url of the server endpoint. Defaults to `http://localhost:5820`
            username (str)
                Username to use in the connection (optional)
            password (str)
                Password to use in the connection (optional)

        Example
            >> admin = Admin(endpoint='http://localhost:9999', username='admin', password='admin')
        """
        self.admin = HTTPAdmin(endpoint, username, password)

    def shutdown(self):
        """
        Shutdown the server
        """
        self.admin.shutdown()

    def database(self, name):
        """
        Retrieve an object representing a database

        Arguments
            name (str)
                Database name

        Returns
            (Database)
                Database object
        """
        return Database(self.admin.database(name))

    def databases(self):
        """
        Retrieve all databases

        Returns
            (list[Database])
                List of Database objects
        """
        return list(map(Database, self.admin.databases()))

    def new_database(self, name, options=None, *contents):
        """
        Create a new database

        Parameters
            name (str)
                Database name
            options (dict)
                Dictionary with database options (optional)
            contents (Content) or ((Content, str))
                List of datasets to perform bulk-load with, optionally with desired named graph (optional)

        Returns
            (Database)
                Database object

        Example
            # options
            >> admin.new_database('db', {'search.enabled': True})

            # bulk-load
            >> admin.new_database('db', {}, File('example.ttl'), File('test.rdf'))

            # bulk-load to named graph
            >> admin.new_database('db', {}, (File('test.rdf'), 'urn:context'))
        """
        files = []

        with ExitStack() as stack:
            for c in contents:
                content = c[0] if isinstance(c, tuple) else c
                context = c[1] if isinstance(c, tuple) else None

                # we will be opening references to many sources in a single call
                # use a stack manager to make sure they all get properly closed at the end
                data = stack.enter_context(content.data())

                files.append({
                    'name': content.name,
                    'content': data,
                    'content-type': content.content_type,
                    'content-encoding': content.content_encoding,
                    'context': context
                })

            return Database(self.admin.new_database(name, options, *files))

    def query(self, id):
        """
        Get information about running query

        Parameters
            id (str)
                Query ID

        Returns
            (dict)
                Query information
        """
        return self.admin.query(id)

    def queries(self):
        """
        Get information about all running queries

        Returns
            (dict)
                Query information
        """
        return self.admin.queries()

    def kill_query(self, id):
        """
        Kill running query

        Parameters
            id (str)
                Query ID
        """
        self.admin.kill_query(id)

    def user(self, name):
        """
        Retrieve object representing an user

        Parameters
            name (str)
                Username

        Returns
            (User)
                User object
        """
        return User(self.admin.user(name))

    def users(self):
        """
        Retrieve all users

        Returns
            (list[User])
                User objects
        """
        return list(map(User, self.admin.users()))

    def new_user(self, username, password, superuser=False):
        """
        Create new user

        Parameters
            username (str)
                Username
            password (str)
                Password
            superuser (bool)
                If user is a super duper user. Defaults to False

        Returns
            (User)
                User object
        """
        return User(self.admin.new_user(username, password, superuser))

    def role(self, name):
        """
        Retrieve object representing role

        Parameters
            name (str)
                Role name

        Returns
            (Role)
                Role object
        """
        return Role(self.admin.role(name))

    def roles(self):
        """
        Retrieve all roles

        Returns
            (list[Role])
                Role objects
        """
        return list(map(Role, self.admin.roles()))

    def new_role(self, name):
        """
        Create new role

        Parameters
            name (str)
                Role name

        Returns
            (Role)
                Role object
        """
        return Role(self.admin.new_role(name))

    def virtual_graph(self, name):
        """
        Retrieve Virtual Graph

        Parameters
            name (str)
                Virtual Graph name

        Returns
            (VirtualGraph)
                Virtual Graph object
        """
        return VirtualGraph(self.admin.virtual_graph(name))

    def virtual_graphs(self):
        """
        Retrieve all virtual graphs

        Returns
            (list[VirtualGraph])
                Virtual Graph objects
        """
        return list(map(VirtualGraph, self.admin.virtual_graphs()))

    def new_virtual_graph(self, name, mappings, options):
        """
        Create new Virtual Graph

        Parameters
            name (str)
                Name
            mappings (Content)
                Mapping contents
            options (dict)
                Options

        Example
            >> admin.new_virtual_graph('users', File('mappings.ttl'), {'jdbc.driver': 'com.mysql.jdbc.Driver'})
        """
        with mappings.data() as data:
            return VirtualGraph(self.admin.new_virtual_graph(name, data.read().decode() if hasattr(data, 'read') else data, options))

    def validate(self):
        """
        Validate admin connection

        Returns
            (bool)
                Connection state
        """
        return self.admin.validate()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.admin.__exit__(*args)


class Database(object):
    """
    Database

    See Also
        https://www.stardog.com/docs/#_database_admin
    """

    def __init__(self, db):
        self.db = db

    @property
    def name(self):
        return self.db.name

    def get_options(self, *options):
        """
        Get database options

        Parameters
            options (str)
                Database option names

        Returns
            (dict)
                Database options

        Example
            >> db.get_options('search.enabled', 'spatial.enabled')
        """
        return self.db.get_options(*options)

    def set_options(self, options):
        """
        Set database options.
        Database must be offline

        Parameters
            options (dict)
                Database options

        Example
            >> db.set_options({'spatial.enabled': False})
        """
        self.db.set_options(options)

    def optimize(self):
        """
        Optimize database
        """
        self.db.optimize()

    def repair(self):
        """
        Repair database
        Database must be offline
        """
        self.db.repair()

    def online(self):
        """
        Set database to online state
        """
        self.db.online()

    def offline(self):
        """
        Set database to offline state
        """
        self.db.offline()

    def copy(self, to):
        """
        Make a copy of this database under another name.
        Database must be offline

        Parameters
            to (str)
                Name of the new database to be created

        Returns
            (Database)
                Object representing the new database
        """
        db = self.db.copy(to)
        return Database(db)

    def drop(self):
        """
        Drop the database
        """
        self.db.drop()


class User(object):
    """
    User

    See Also
        https://www.stardog.com/docs/#_security
    """

    def __init__(self, user):
        self.user = user

    @property
    def name(self):
        return self.user.name

    def set_password(self, password):
        """
        Set new password

        Parameters
            password (str)
                Password
        """
        self.user.set_password(password)

    def is_enabled(self):
        """
        Check if user is enabled

        Returns
            (bool)
                User activation state
        """
        return self.user.is_enabled()

    def set_enabled(self, enabled):
        """
        Enable/disable user

        Parameters
            enabled (bool)
                User state
        """
        self.user.set_enabled(enabled)

    def is_superuser(self):
        """
        Check if user is super duper user

        Returns
            (bool)
                Superuser state
        """
        return self.user.is_superuser()

    def roles(self):
        """
        Get all roles from this user

        Returns
            (list[Role])
                User roles
        """
        return list(map(Role, self.user.roles()))

    def add_role(self, role):
        """
        Add existing role to user

        Params
            role (str or Role)
                The role to add

        Example
            >> user.add_role('reader')
            >> user.add_role(admin.role('reader'))
        """
        self.user.add_role(self.__rolename(role))

    def set_roles(self, *roles):
        """
        Set the roles of this user

        Params
            roles (str or Role)
                The roles to add

        Example
            >> user.set_roles('reader', admin.role('writer'))
        """
        self.user.set_roles(*list(map(self.__rolename, roles)))

    def remove_role(self, role):
        """
        Remove role from user

        Params
            role (str or Role)
                The role to add

        Example
            >> user.add_role('reader')
            >> user.add_role(admin.role('reader'))
        """
        self.user.remove_role(self.__rolename(role))

    def delete(self):
        """
        Delete user
        """
        self.user.delete()

    def permissions(self):
        """
        Get user permissions

        Returns
            (dict)
                User permissions

        See Also
            https://www.stardog.com/docs/#_permissions
        """
        return self.user.permissions()

    def add_permission(self, action, resource_type, resource):
        """
        Add permission to user

        Parameters
            action (str)
                Action type (e.g., 'read', 'write')
            resource_type (str)
                Resource type (e.g., 'user', 'db')
            resource (str)
                Target resource (e.g., 'username', '*')

        Example
            >> user.add_permission('read', 'user', 'username')
            >> user.add_permission('write', '*', '*')

        See Also
            https://www.stardog.com/docs/#_permissions
        """
        self.user.add_permission(action, resource_type, resource)

    def remove_permission(self, action, resource_type, resource):
        """
        Remove permission from user

        Parameters
            action (str)
                Action type (e.g., 'read', 'write')
            resource_type (str)
                Resource type (e.g., 'user', 'db')
            resource (str)
                Target resource (e.g., 'username', '*')

        Example
            >> user.remove_permission('read', 'user', 'username')
            >> user.remove_permission('write', '*', '*')

        See Also
            https://www.stardog.com/docs/#_permissions
        """
        self.user.remove_permission(action, resource_type, resource)

    def effective_permissions(self):
        """
        Get user's effective permissions

        Returns
            (dict)
                User effective permissions
        """
        return self.user.effective_permissions()

    def __rolename(self, role):
        return role.name if isinstance(role, Role) else role


class Role(object):
    """
    Role

    See Also
        https://www.stardog.com/docs/#_security
    """

    def __init__(self, role):
        self.role = role

    @property
    def name(self):
        return self.role.name

    def users(self):
        """
        List of users for this role

        Returns
            (list[User])
                Users
        """
        return list(map(User, self.role.users()))

    def delete(self, force=None):
        """
        Delete role

        Parameters
            force (bool)
                Force deletion of role
        """
        self.role.delete(force)

    def permissions(self):
        """
        Get role permissions

        Returns
            (dict)
                Role permissions

        See Also
            https://www.stardog.com/docs/#_permissions
        """
        return self.role.permissions()

    def add_permission(self, action, resource_type, resource):
        """
        Add permission to role

        Parameters
            action (str)
                Action type (e.g., 'read', 'write')
            resource_type (str)
                Resource type (e.g., 'user', 'db')
            resource (str)
                Target resource (e.g., 'username', '*')

        Example
            >> role.add_permission('read', 'user', 'username')
            >> role.add_permission('write', '*', '*')

        See Also
            https://www.stardog.com/docs/#_permissions
        """
        self.role.add_permission(action, resource_type, resource)

    def remove_permission(self, action, resource_type, resource):
        """
        Remove permission from role

        Parameters
            action (str)
                Action type (e.g., 'read', 'write')
            resource_type (str)
                Resource type (e.g., 'user', 'db')
            resource (str)
                Target resource (e.g., 'username', '*')

        Example
            >> role.remove_permission('read', 'user', 'username')
            >> role.remove_permission('write', '*', '*')

        See Also
            https://www.stardog.com/docs/#_permissions
        """
        self.role.remove_permission(action, resource_type, resource)


class VirtualGraph(object):
    """
    Virtual Graph

    See Also
        https://www.stardog.com/docs/#_structured_data
    """

    def __init__(self, vg):
        self.vg = vg

    @property
    def name(self):
        return self.vg.name

    def update(self, name, mappings, options):
        """
        Update Virtual Graph

        Parameters
            name (str)
                New name
            mappings (Content)
                New mapping contents
            options (dict)
                New options

        Example
            >> vg.update('users', File('mappings.ttl'), {'jdbc.driver': 'com.mysql.jdbc.Driver'})
        """
        with mappings.data() as data:
            self.vg.update(name, data.read().decode() if hasattr(data, 'read') else data, options)

    def delete(self):
        """
        Delete Virtual Graph
        """
        self.vg.delete()

    def options(self):
        """
        Get Virtual Graph options

        Returns
            (dict)
                Options
        """
        return self.vg.options()

    def mappings(self, content_type=TURTLE):
        """
        Get Virtual Graph mappings

        Parameters
            content_type (str)
                Content type for results. Defaults to 'text/turtle'

        Returns
            (str)
                Mappings in given content type
        """
        return self.vg.mappings(content_type)

    def available(self):
        """
        Check if Virtual Graph is available

        Returns
            (bool)
                Availability state
        """
        return self.vg.available()
