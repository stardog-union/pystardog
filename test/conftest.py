import pytest
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
        "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?useSSL=false",
    }
    return options


@pytest.fixture
def videos_options():
    options = {
        "jdbc.driver": "com.mysql.jdbc.Driver",
        "jdbc.username": "user",
        "jdbc.password": "pass",
        "mappings.syntax": "STARDOG",
        "jdbc.url": "jdbc:mysql://pystardog_mysql_videos/videos?useSSL=false",
    }
    return options
