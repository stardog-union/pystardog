import pytest
import datetime
import os


import stardog.admin
import stardog.connection as connection
import stardog.content as content
import stardog.content_types as content_types
import stardog.exceptions as exceptions

DEFAULT_USERS = ['admin', 'anonymous']
DEFAULT_ROLES = ['reader']


@pytest.fixture()
def admin(conn_string):
    with stardog.admin.Admin(**conn_string) as admin:

        # IMO we should remove these. reason being, if by mistake a user decides to run this
        # tests against their own stardog deployment, and they don't go trough the code first,
        # they are risking dropping all databases, users, roles, and others.

        # alternatively we could warn somewhere (README, when running the tests, or other places)
        # that this are not intended to run against any other stardog deployment that is not a clean one.
        for db in admin.databases():
            db.drop()

        for user in admin.users():
            if user.name not in DEFAULT_USERS:
                user.delete()

        for role in admin.roles():
            if role.name not in DEFAULT_ROLES:
                role.delete()

        for stored_query in admin.stored_queries():
            stored_query.delete()

        for vg in admin.virtual_graphs():
            vg.delete()

        for ds in admin.datasources():
            ds.delete()

        yield admin


def test_databases(admin, conn_string):
    assert len(admin.databases()) == 0

    # create database
    db = admin.new_database('db', {
        'search.enabled': True,
        'spatial.enabled': True
    })

    assert len(admin.databases()) == 1
    assert db.name == 'db'
    assert db.get_options('search.enabled', 'spatial.enabled') == {
        'search.enabled': True,
        'spatial.enabled': True
    }

    # change options
    db.offline()
    db.set_options({'spatial.enabled': False})
    db.online()

    assert db.get_options('search.enabled', 'spatial.enabled') == {
        'search.enabled': True,
        'spatial.enabled': False
    }

    # optimize
    db.optimize()

    # repair
    db.offline()
    db.repair()
    db.online()

    # bulk load
    contents = [
        content.Raw(
            '<urn:subj> <urn:pred> <urn:obj3> .',
            content_types.TURTLE,
            name='bulkload.ttl'),
        (content.File('test/data/example.ttl.zip'), 'urn:context'),
        content.URL('https://www.w3.org/2000/10/rdf-tests/'
                    'RDF-Model-Syntax_1.0/ms_4.1_1.rdf')
    ]

    bl = admin.new_database('bulkload', {}, *contents)

    with connection.Connection(
            'bulkload', **conn_string) as c:
        q = c.select('select * where { graph <urn:context> {?s ?p ?o}}')
        assert len(q['results']['bindings']) == 1
        assert c.size() == 7

    # clear
    db.drop()
    bl.drop()

    assert len(admin.databases()) == 0


def test_backup_and_restore(admin, conn_string):
    def check_db_for_contents(dbname, num_results):
        with connection.Connection(
                dbname, **conn_string) as c:
            q = c.select('select * where { ?s ?p ?o }')
            assert len(q['results']['bindings']) == num_results

    # determine the path to the backups
    now = datetime.datetime.now()
    date = now.strftime('%Y-%m-%d')
    # This only makes sense if stardog server is running in the same server. Recommended STARDOG_HOME
    # should be /var/opt/stardog which I am setting as default in case of hitting a remote server.
    stardog_home = os.getenv('STARDOG_HOME', '/var/opt/stardog')
    restore_from = os.path.join(
        f"{stardog_home}", '.backup', 'backup_db', f"{date}")

    # make a db with test data loaded
    db = admin.new_database(
        'backup_db', {}, content.File('test/data/starwars.ttl'))

    db.backup()
    db.drop()

    # data is back after restore
    try:
        admin.restore(from_path=restore_from)
    except Exception as e:
        raise Exception(str(e) + ". Check whether $STARDOG_HOME is set")
    check_db_for_contents('backup_db', 87)

    # error if attempting to restore over an existing db without force
    with pytest.raises(
            exceptions.StardogException,
            match='Database already exists'):
        admin.restore(from_path=restore_from)

    # restore to a new db
    admin.restore(from_path=restore_from, name='backup_db2')
    check_db_for_contents('backup_db2', 87)

    # force to overwrite existing
    db.drop()
    db = admin.new_database('backup_db')
    check_db_for_contents('backup_db', 0)
    admin.restore(from_path=restore_from, force=True)
    check_db_for_contents('backup_db', 87)

    # the backup location can be specified
    db.backup(to=os.path.join(stardog_home, 'backuptest'))

    # clean up
    db.drop()
    admin.database('backup_db2').drop()


def test_users(admin, conn_string):
    assert len(admin.users()) == len(DEFAULT_USERS)

    # new user
    user = admin.new_user('username', 'password', False)

    assert len(admin.users()) == len(DEFAULT_USERS) + 1
    assert not user.is_superuser()
    assert user.is_enabled()

    # check if able to connect
    with stardog.admin.Admin(**conn_string) as uadmin:
        uadmin.validate()

    # change password
    user.set_password('new_password')
    with stardog.admin.Admin(endpoint=conn_string['endpoint'],
            username='username', password='new_password') as uadmin:
        uadmin.validate()

    # disable/enable
    user.set_enabled(False)
    assert not user.is_enabled()
    user.set_enabled(True)
    assert user.is_enabled()

    # roles
    assert len(user.roles()) == 0

    user.add_role('reader')
    roles = user.roles()
    assert len(user.roles()) == 1

    user.set_roles(*roles)
    assert len(user.roles()) == 1

    user.remove_role('reader')
    assert len(user.roles()) == 0

    # permissions
    assert user.permissions() == [{
        'action': 'READ',
        'resource_type': 'user',
        'resource': ['username']
    },
        {'action': 'WRITE',
         'resource_type': 'user',
         'resource': ['username']}
    ]
    assert user.effective_permissions() == [{
        'action': 'READ',
        'resource_type': 'user',
        'resource': ['username']
    },
        {'action': 'WRITE',
         'resource_type': 'user',
         'resource': ['username']}
    ]

    # delete user
    user.delete()

    assert len(admin.users()) == len(DEFAULT_USERS)


def test_roles(admin):
    assert len(admin.roles()) == len(DEFAULT_ROLES)

    # users
    role = admin.role('reader')
    assert len(role.users()) > 0

    # new role
    role = admin.new_role('writer')
    assert len(admin.roles()) == len(DEFAULT_ROLES) + 1

    # permissions
    assert role.permissions() == []

    role.add_permission('WRITE', '*', '*')
    assert role.permissions() == [{
        'action': 'WRITE',
        'resource_type': '*',
        'resource': ['*']
    }]

    role.remove_permission('WRITE', '*', '*')
    assert role.permissions() == []

    # remove role
    role.delete()

    assert len(admin.roles()) == len(DEFAULT_ROLES)


def test_queries(admin):
    assert len(admin.queries()) == 0

    with pytest.raises(
            exceptions.StardogException,
            match='Query not found: 1'):
        admin.query(1)

    with pytest.raises(
            exceptions.StardogException,
            match='Query not found: 1'):
        admin.kill_query(1)


def test_stored_queries(admin):
    query = 'select * where { ?s ?p ?o . }'
    assert len(admin.stored_queries()) == 0

    with pytest.raises(
            exceptions.StardogException,
            match='Stored query not found'):
        admin.stored_query('not a real stored query')

    # add a stored query
    stored_query = admin.new_stored_query('everything', query)
    assert 'everything' in [sq.name for sq in admin.stored_queries()]

    # get a stored query
    stored_query_copy = admin.stored_query('everything')
    assert stored_query_copy.query == query

    # update a stored query
    assert stored_query.description is None
    stored_query.update(description='get all the triples')
    assert stored_query.description == 'get all the triples'

    # delete a stored query
    stored_query.delete()
    assert len(admin.stored_queries()) == 0

    # clear the stored queries
    stored_query = admin.new_stored_query('everything', query)
    stored_query = admin.new_stored_query('everything2', query)
    assert len(admin.stored_queries()) == 2
    admin.clear_stored_queries()
    assert len(admin.stored_queries()) == 0

def test_virtual_graphs(admin, music_options):
    assert len(admin.virtual_graphs()) == 0

    #creating a VG using empty mappings, and specifying a datasource
    ds = admin.new_datasource('music', music_options)
    vg = admin.new_virtual_graph('test_vg', mappings='', datasource=ds.name)
    vg.delete()
    ds.delete()

    # existing datasource name, plus dbname, plus empty mappings#
    ds = admin.new_datasource('music', music_options)
    vg = admin.new_virtual_graph('test_vg', mappings='', datasource=ds.name, db='somedb')
    vg.delete()
    ds.delete()

    # test passing options instead of a datasource
    vg = admin.new_virtual_graph('test_vg', mappings='', options=music_options)
    vg.delete()


    # specify a mappings file, and options.
    vg = admin.new_virtual_graph('test_vg', mappings=content.File('test/data/music_mappings.ttl'), options=music_options)
    vg.delete()

    # passing options and a datasource should not conflict
    ds = admin.new_datasource('music', music_options)
    vg = admin.new_virtual_graph('test_vg', mappings='', options=music_options, datasource=ds.name, db='no-db')
    vg.delete()
    ds.delete()


    # Testing basic errors
    vg = admin.virtual_graph('test')

    with pytest.raises(TypeError, match="missing 1 required positional argument: 'mappings'"):
        admin.new_virtual_graph('test_vg', datasource='non-existent')

    with pytest.raises(exceptions.StardogException, match='Unable to determine data source type'):
        admin.new_virtual_graph('vg', content.File('test/data/r2rml.ttl'))

    with pytest.raises(exceptions.StardogException, match='Unable to determine data source type'):
        vg.update('vg', content.File('test/data/r2rml.ttl'))

    with pytest.raises(exceptions.StardogException, match='Virtual Graph test Not Found!'):
        vg.available()
        vg.options()
        vg.mappings()
        vg.delete()


def count_records(bd_name, conn_string):

    with connection.Connection(bd_name, **conn_string) as conn:
        graph_name = conn.select('select ?g { graph ?g {}}')['results']['bindings'][0]['g']['value']
        q = conn.select('SELECT * { GRAPH <' + graph_name + '> { ?s ?p ?o }}')
        count = len(q['results']['bindings'])
    return count


def test_import(admin, conn_string, music_options, videos_options):

    bd = admin.new_database('test-db')
    graph_name = 'test-graph'

    # tests passing mappings
    admin.import_virtual_graph('test-db', mappings=content.File('test/data/music_mappings.ttl'), named_graph=graph_name, remove_all=True, options=music_options)
    # specified mapping file generates a graph with total of 37 triples
    assert 37 == count_records(bd.name, conn_string)

    # tests passing empty mappings
    admin.import_virtual_graph('test-db', mappings='', named_graph=graph_name, remove_all=True, options=music_options)
    # if mapping is not specified, music bd generates a graph with 79 triples
    assert 79 == count_records(bd.name, conn_string)

    # test removing_all false, it should return  music records + video records.
    admin.import_virtual_graph('test-db', mappings='', named_graph=graph_name, remove_all=False, options=videos_options)
    # if no mapping is specified, videos db generates a graph with 800 triples. adding un-mapped music sums up to 879.
    assert 879 == count_records(bd.name, conn_string)

def test_data_source(admin, music_options):

    ds = admin.new_datasource('music', music_options)
    assert len(admin.datasources()) == 1
    assert ds.name == 'music'
    assert ds.get_options() == music_options
    ds.delete()
    assert len(admin.datasources()) == 0




