import pytest
import datetime
import os
from time import sleep

import stardog.admin
import stardog.connection as connection
import stardog.content as content
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
        databases = admin.databases()
        if databases:
            for db in admin.databases():
                db.drop()

        users = admin.users()
        if users:
            for user in users:
                if user.name not in DEFAULT_USERS:
                    user.delete()

        roles = admin.roles()
        if roles:
            for role in roles:
                if role.name not in DEFAULT_ROLES:
                    role.delete()

        stored_queries = admin.stored_queries()
        if stored_queries:
            for stored_query in stored_queries:
                stored_query.delete()

        cache_targets = admin.cache_targets()
        if cache_targets :
            for cache in cache_targets:
                cache.remove()

        yield admin


def test_databases(admin, conn_string, bulkload_content):
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

    # verify
    db.verify()

    # repair
    db.offline()
    db.repair()
    db.online()


    bl = admin.new_database('bulkload', {}, *bulkload_content)

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


def wait_for_cleaning_cache_target(admin, cache_target_name):
    retries = 0
    while True:
        cache_targets = admin.cache_targets()
        cache_target_names = [cache_target.name for cache_target in cache_targets]
        if cache_target_name in cache_target_names:
            retries += 1
            sleep(1)
            if retries >= 20:
                raise Exception("Took too long to remove cache target: " + cache_target_name)
        else:
            return

def wait_for_creating_cache_target(admin, cache_target_name):
    retries = 0
    while True:
        cache_targets = admin.cache_targets()
        cache_target_names = [cache_target.name  for cache_target in cache_targets]
        if cache_target_name not in cache_target_names:
            retries += 1
            sleep(1)
            if retries >= 20:
                raise Exception("Took too long to register cache target: " + cache_target_name)
        else:
            return


def test_cache_targets(admin, cache_target_info):

    cache_target_name = cache_target_info['target_name']
    cache_target_hostname = cache_target_info['hostname']
    cache_target_port = cache_target_info['port']
    cache_target_username = cache_target_info['username']
    cache_target_password = cache_target_info['password']

    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 0

    cache_target = admin.new_cache_target(cache_target_name, cache_target_hostname, cache_target_port, cache_target_username, cache_target_password)
    wait_for_creating_cache_target(admin, cache_target_name)
    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 1

    assert cache_target.info()['name'] == cache_target_name
    assert cache_target.info()['port'] == cache_target_port
    assert cache_target.info()['hostname'] == cache_target_hostname
    assert cache_target.info()['username'] == cache_target_username


    # test remove()
    cache_target.remove()
    wait_for_cleaning_cache_target(admin, cache_target.name)
    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 0


    # tests orphan
    cache_target = admin.new_cache_target(cache_target_name, cache_target_hostname, cache_target_port, cache_target_username, cache_target_password)
    wait_for_creating_cache_target(admin, cache_target_name)
    cache_target.orphan()
    wait_for_cleaning_cache_target(admin, cache_target.name)
    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 0
    # recall that removing a cache is an idempotent operation, and will always (unless it fails)
    # return that cache target was removed, even if it doesn't exists in the first place
    # Removing a an orphaned cache target will not delete the data, because the target is already orphaned
    # We need to recreate the orphaned cached target (using use_existing_db) in order to fully delete its data
    cache_target = admin.new_cache_target(cache_target_name, cache_target_hostname, cache_target_port, cache_target_username, cache_target_password, use_existing_db=True)
    wait_for_creating_cache_target(admin, cache_target_name)
    cache_target.remove()

def test_cache_ng_datasets(admin, bulkload_content, cache_target_info):

    cache_target_name = cache_target_info['target_name']
    cache_target_hostname = cache_target_info['hostname']
    cache_target_port = cache_target_info['port']
    cache_target_username = cache_target_info['username']
    cache_target_password = cache_target_info['password']

    cache_target = admin.new_cache_target(cache_target_name, cache_target_hostname, cache_target_port, cache_target_username, cache_target_password)
    wait_for_creating_cache_target(admin, cache_target_name)

    bl = admin.new_database('bulkload', {}, *bulkload_content)

    assert len(admin.cached_graphs()) == 0
    cached_graph_name = 'cache://cached-ng'
    cached_graph = admin.new_cached_graph(cached_graph_name, cache_target.name, 'urn:context', bl.name)

    assert len(admin.cached_graphs()) == 1

    cached_graph_status = cached_graph.status()
    assert cached_graph_status[0]['name'] == cached_graph_name
    assert cached_graph_status[0]['target'] == cache_target.name
    cached_graph.refresh()
    cached_graph.drop()
    assert len(admin.cached_queries()) == 0



def test_cache_vg_datasets(admin, music_options, cache_target_info):

    cache_target_name = cache_target_info['target_name']
    cache_target_hostname = cache_target_info['hostname']
    cache_target_port = cache_target_info['port']
    cache_target_username = cache_target_info['username']
    cache_target_password = cache_target_info['password']

    #creating a VG using empty mappings, and specifying a datasource
    ds = admin.new_datasource('music', music_options)
    admin.new_virtual_graph('test_vg', mappings='', datasource=ds.name)

    cache_target = admin.new_cache_target(cache_target_name, cache_target_hostname, cache_target_port, cache_target_username, cache_target_password)
    wait_for_creating_cache_target(admin, cache_target_name)
    # We need to register the VG into the cache target as well.

    conn_string_cache = {
        'endpoint': 'http://' +  cache_target_hostname + ':5820',
        'username': 'admin',
        'password': 'admin'
    }

    with stardog.admin.Admin(**conn_string_cache) as admin_cache_target:
        ds = admin_cache_target.new_datasource('music', music_options)
        vg = admin_cache_target.new_virtual_graph('test_vg', mappings='', datasource=ds.name)

    assert len(admin.cached_graphs()) == 0


    cached_graph_name = 'cache://cached-vg'
    cached_graph = admin.new_cached_graph(cached_graph_name, cache_target.name, 'virtual://' + vg.name)

    assert len(admin.cached_graphs()) == 1

    cached_graph_status = cached_graph.status()
    assert cached_graph_status[0]['name'] == cached_graph_name
    assert cached_graph_status[0]['target'] == cache_target.name
    cached_graph.refresh()
    cached_graph.drop()
    assert len(admin.cached_queries()) == 0

    cache_target.remove()
    wait_for_cleaning_cache_target(admin, cache_target.name)

    vg.delete()
    ds.delete()

def test_cache_query_datasets(admin, bulkload_content, cache_target_info):

    cache_target_name = cache_target_info['target_name']
    cache_target_hostname = cache_target_info['hostname']
    cache_target_port = cache_target_info['port']
    cache_target_username = cache_target_info['username']
    cache_target_password = cache_target_info['password']

    cache_target = admin.new_cache_target(cache_target_name, cache_target_hostname, cache_target_port, cache_target_username, cache_target_password)
    wait_for_creating_cache_target(admin, cache_target_name)

    bl = admin.new_database('bulkload', {}, *bulkload_content)

    assert len(admin.cached_queries()) == 0

    cached_query_name = 'cache://my_new_query_cached'

    cached_query = admin.new_cached_query(cached_query_name, cache_target.name,  'SELECT * { ?s ?p ?o }', bl.name)
    #wait_for_creating_cache_dataset(admin, cached_query_name, 'query')

    assert len(admin.cached_queries()) == 1

    cached_query_status = cached_query.status()
    assert cached_query_status[0]['name'] == cached_query_name
    assert cached_query_status[0]['target'] ==cache_target.name
    cached_query.refresh()
    cached_query.drop()
    assert len(admin.cached_queries()) == 0


    cache_target.remove()
    wait_for_cleaning_cache_target(admin, cache_target.name)