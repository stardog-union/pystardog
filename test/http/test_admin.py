import pytest

from stardog.http.client import Client
from stardog.http.admin import Admin
from stardog.http.connection import Connection
from stardog.exceptions import StardogException
from stardog.content_types import TURTLE

DEFAULT_USERS = ['admin', 'anonymous', 'root']
DEFAULT_ROLES = ['reader']

@pytest.fixture(scope="module")
def conn():
    conn = Admin(username='admin', password='admin')

    for db in conn.databases():
        db.drop()

    for user in conn.users():
        if user.name not in DEFAULT_USERS:
            user.delete()
    
    for role in conn.roles():
        if role.name not in DEFAULT_ROLES:
            role.delete()
    
    return conn


def test_databases(conn):
    assert len(conn.databases()) == 0

    # create database
    db = conn.new_database('db', {'search.enabled': True, 'spatial.enabled': True})

    assert len(conn.databases()) == 1
    assert db.name == 'db'
    assert db.get_options('search.enabled', 'spatial.enabled') == {'search.enabled': True, 'spatial.enabled': True}

    # change options
    db.offline()
    db.set_options({'spatial.enabled': False})
    db.online()

    assert db.get_options('search.enabled', 'spatial.enabled') == {'search.enabled': True, 'spatial.enabled': False}

    # optimize
    db.optimize()

    # repair
    db.offline()
    db.repair()
    db.online()

    # copy to new database
    db.offline()
    copy = db.copy('copy')

    assert len(conn.databases()) == 2
    assert copy.name == 'copy'
    assert copy.get_options('search.enabled', 'spatial.enabled') == {'search.enabled': True, 'spatial.enabled': False}

    # bulk load
    bl = conn.new_database('bulkload', {}, [{'name': 'example.ttl', 'content': open('test/data/example.ttl', 'rb'), 'content-type': TURTLE, 'context': '<urn:a>'}])
    c = Connection('bulkload', username='admin', password='admin')
    assert c.size() == 1

    # clear
    copy.drop()
    db.drop()
    bl.drop()

    assert len(conn.databases()) == 0

def test_users(conn):
    assert len(conn.users()) == len(DEFAULT_USERS)

    # new user
    user = conn.new_user('username', 'password', False)

    assert len(conn.users()) == len(DEFAULT_USERS) + 1
    assert user.is_superuser() == False
    assert user.is_enabled() == True

    # check if able to connect
    uconn = Admin(username='username', password='password')
    uconn.validate()

    # change password
    user.set_password('new_password')
    uconn = Admin(username='username', password='new_password')
    uconn.validate()

    # disable/enable
    user.set_enabled(False)
    assert user.is_enabled() == False
    user.set_enabled(True)
    assert user.is_enabled() == True

    # roles
    assert len(user.roles()) == 0

    user.add_role('reader')
    roles = user.roles()
    assert len(roles) == 1

    user.remove_role('reader')
    assert len(user.roles()) == 0

    user.set_roles(*roles)
    assert len(user.roles()) == 1

    user.remove_role(roles[0])
    assert len(user.roles()) == 0

    # permissions
    assert user.permissions() == [{'action': 'READ', 'resource_type': 'user', 'resource': ['username']}]
    assert user.effective_permissions() == [{'action': 'READ', 'resource_type': 'user', 'resource': ['username']}]

    user.add_permission('WRITE', 'user', 'username')
    assert user.permissions() == [{'action': 'READ', 'resource_type': 'user', 'resource': ['username']}, {'action': 'WRITE', 'resource_type': u'user', 'resource': ['username']}]

    user.remove_permission('WRITE', 'user', 'username')
    assert user.permissions() == [{'action': 'READ', 'resource_type': 'user', 'resource': ['username']}]

    # delete user
    user.delete()

    assert len(conn.users()) == len(DEFAULT_USERS)

def test_roles(conn):
    assert len(conn.roles()) == len(DEFAULT_ROLES)

    # users
    role = conn.role('reader')
    assert len(role.users()) > 0

    # new role
    role = conn.new_role('writer')
    assert len(conn.roles()) == len(DEFAULT_ROLES) + 1

    # permissions
    assert role.permissions() == []

    role.add_permission('WRITE', '*', '*')
    assert role.permissions() == [{'action': 'WRITE', 'resource_type': '*', 'resource': ['*']}]

    role.remove_permission('WRITE', '*', '*')
    assert role.permissions() == []

    # remove role
    role.delete()

    assert len(conn.roles()) == len(DEFAULT_ROLES)

def test_virtual_graphs(conn):

    assert len(conn.virtual_graphs()) == 0

    mappings = '_:1 a <http://www.w3.org/ns/r2rml#ObjectMap> ._:2 <http://www.w3.org/ns/r2rml#predicate> <http://example.com/dept/deptno> .'

    options = {
        "namespaces": "stardog=tag:stardog:api",
        "jdbc.driver": "com.mysql.jdbc.Driver",
        "jdbc.username": "admin",
        "jdbc.password": "admin",
        "jdbc.url": "jdbc:mysql://localhost/support"
    }

    vg = conn.virtual_graph('test')

    # TODO add VG to test server

    with pytest.raises(StardogException, match='com.mysql.cj.jdbc.exceptions.CommunicationsException'):
        conn.new_virtual_graph('vg', mappings, options)

    with pytest.raises(StardogException, match='com.mysql.cj.jdbc.exceptions.CommunicationsException'):
        vg.update('vg', mappings, options)
    
    with pytest.raises(StardogException, match='Virtual Graph test Not Found!'):
        vg.available()

    with pytest.raises(StardogException, match='Virtual Graph test Not Found!'):
        vg.options()
    
    with pytest.raises(StardogException, match='Virtual Graph test Not Found!'):
        vg.mappings()
    
    with pytest.raises(StardogException, match='Virtual Graph test Not Found!'):
        vg.delete()

