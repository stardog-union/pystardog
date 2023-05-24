import pytest
import stardog
import stardog.content as content
import stardog.content_types as content_types
import os


# STARDOG_ENDPOINT = os.environ.get('STARDOG_ENDPOINT', None)
STARDOG_HOSTNAME_NODE_1 = os.environ.get("STARDOG_HOSTNAME_NODE_1", None)
STARDOG_HOSTNAME_CACHE = os.environ.get("STARDOG_HOSTNAME_CACHE", None)
STARDOG_HOSTNAME_STANDBY = os.environ.get("STARDOG_HOSTNAME_STANDBY", None)


def pytest_addoption(parser):
    parser.addoption("--username", action="store", default="admin")
    parser.addoption("--passwd", action="store", default="admin")
    parser.addoption("--endpoint", action="store", default="http://localhost:5820")
    parser.addoption("--http_proxy", action="store", default="")
    parser.addoption("--https_proxy", action="store", default="")
    parser.addoption("--ssl_verify", action="store_true", default=True)


@pytest.fixture
def conn_string(pytestconfig):
    conn = {
        "endpoint": pytestconfig.getoption("endpoint"),
        "username": pytestconfig.getoption("username"),
        "password": pytestconfig.getoption("passwd"),
    }
    return conn


@pytest.fixture
def proxies(pytestconfig):
    proxies_config = {}
    for protocol in ("http", "https"):
        proxy_url = pytestconfig.getoption(f"{protocol}_proxy")
        if (
            proxy_url is not None
            and isinstance(proxy_url, str)
            and proxy_url.startswith(protocol)
        ):
            proxies_config.update({protocol: proxy_url})
    return proxies_config


@pytest.fixture
def ssl_verify(pytestconfig):
    return pytestconfig.getoption("ssl_verify")


# this is currently not being used, need to confirm which one is definitive.
@pytest.fixture
def bulkload_content():
    contents = [
        content.Raw(
            "<urn:subj> <urn:pred> <urn:obj3> .",
            content_types.TURTLE,
            name="bulkload.ttl",
        ),
        (content.File("test/data/example.ttl.zip"), "urn:context"),
        content.URL(
            "https://www.w3.org/2000/10/rdf-tests/" "RDF-Model-Syntax_1.0/ms_4.1_1.rdf"
        ),
    ]
    return contents


@pytest.fixture
def cache_target_info():
    target_info = {
        "target_name": "pystardog-test-cache-target",
        "hostname": STARDOG_HOSTNAME_CACHE,
        "port": 5820,
        "username": "admin",
        "password": "admin",
    }
    return target_info


@pytest.fixture
def cluster_standby_node_conn_string():
    standby_conn_string = {
        "endpoint": f"http://{STARDOG_HOSTNAME_STANDBY}:5820",
        "username": "admin",
        "password": "admin",
    }
    return standby_conn_string


# Java 291 (packed in the Stardog docker image from 7.6.3+) disabled TLS 1.0 and 1.1 which breaks the MySQL connector:
# https://www.oracle.com/java/technologies/javase/8u291-relnotes.html
# ?useSSL=false works around this for testing purposes:
@pytest.fixture
def music_options():
    options = {
        "jdbc.driver": "com.mysql.jdbc.Driver",
        "jdbc.username": "user",
        "jdbc.password": "pass",
        "mappings.syntax": "STARDOG",
        "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?allowPublicKeyRetrieval=true&useSSL=false",
    }
    return options


@pytest.fixture()
def admin(conn_string):
    with stardog.admin.Admin(**conn_string) as admin:
        yield admin


@pytest.fixture
def conn(conn_string, request):
    endpoint = request.node.get_closest_marker("conn_endpoint", None)
    username = request.node.get_closest_marker("conn_username", None)
    password = request.node.get_closest_marker("conn_password", None)

    if endpoint is not None:
        endpoint = endpoint.args[0]
        conn_string["endpoint"] = endpoint

    if username is not None:
        username = username.args[0]
        conn_string["username"] = username

    if password is not None:
        password = password.args[0]
        conn_string[password] = password

    dbname = request.node.get_closest_marker("conn_dbname", None)
    if dbname is not None:
        with stardog.connection.Connection(dbname.args[0], **conn_string) as c:
            yield c
    else:
        raise Exception("A dbname must be passed with the marker: conn_dbname")


# The generic datasource is the music datasource.
@pytest.fixture()
def datasource(admin, music_options, request):
    dsname = request.node.get_closest_marker("ds_name", None)
    options = request.node.get_closest_marker("options", None)
    if dsname is not None:
        dsname = dsname.args[0]
    else:
        dsname = "datasource"

    if options is not None:
        options = options.args[0]
    else:
        options = music_options
    ds = admin.new_datasource(dsname, options)
    yield ds
    ds.delete()


@pytest.fixture()
def virtual_graph(admin, music_options, datasource, request):
    vgname = request.node.get_closest_marker("vgname", None)
    mappings = request.node.get_closest_marker("mappings", None)
    virtual_graph_options = request.node.get_closest_marker(
        "virtual_graph_options", None
    )
    datasource_name = request.node.get_closest_marker("datasource_name", None)
    database_name = request.node.get_closest_marker("database_name", None)
    use_music_datasource = request.node.get_closest_marker("use_music_datasource", None)

    if vgname is not None:
        vgname = vgname.args[0]
    else:
        vgname = "virtual_graph"

    if mappings is not None:
        mappings = mappings.args[0]

    if virtual_graph_options is not None:
        virtual_graph_options = virtual_graph_options.args[0]

    if database_name is not None:
        database_name = database_name.args[0]

    if use_music_datasource:
        datasource_name = datasource.name

    if virtual_graph_options is None and use_music_datasource is None:
        raise Exception(
            "Must pass either a datasource or VG options. See pytest marks in the pystest documentation"
        )

    vg = admin.new_virtual_graph(
        vgname,
        mappings=mappings,
        options=virtual_graph_options,
        datasource=datasource_name,
        db=database_name,
    )
    yield vg
    vg.delete()


@pytest.fixture()
def user(admin, request):
    username = request.node.get_closest_marker("user_username", None)
    password = request.node.get_closest_marker("user_password", None)
    if username is not None:
        username = username.args[0]
    else:
        username = "pystardogUser"

    if password is not None:
        password = password.args[0]
    else:
        password = "pystardogPass"

    user = admin.new_user(username, password, False)
    yield user
    user.delete()


@pytest.fixture()
def role(admin):
    role = admin.new_role("writer")
    yield role
    role.delete()


@pytest.fixture
def db(admin, request):
    dbname = request.node.get_closest_marker("dbname", None)
    options = request.node.get_closest_marker("options", None)
    contents = request.node.get_closest_marker("contents", None)
    kwargs = request.node.get_closest_marker("kwargs", None)

    if dbname is not None:
        dbname = dbname.args[0]
    else:
        dbname = "pystardog-test-db"

    if options is not None:
        options = options.args[0]

    if contents is not None:
        contents = contents.args[0]

    if kwargs is not None:
        kwargs = kwargs.args[0]

    if contents is None and kwargs is None:
        db = admin.new_database(dbname, options)
    if kwargs is None and contents is not None:
        db = admin.new_database(dbname, options, *contents)
    if kwargs is not None and contents is None:
        db = admin.new_database(dbname, options, **kwargs)
    if kwargs is not None and contents is not None:
        db = admin.new_database(dbname, options, *contents, **kwargs)
    yield db
    db.drop()


@pytest.fixture
def stored_query(admin, request):
    query_name = request.node.get_closest_marker("query_name", None)
    query = request.node.get_closest_marker("query", None)

    if query_name is not None:
        query_name = query_name.args[0]
    else:
        query_name = "everything"

    if query is not None:
        query = query.args[0]
    else:
        query = "select * where { ?s ?p ?o . }"
    stored_query = admin.new_stored_query(query_name, query)
    yield stored_query
    stored_query.delete()
