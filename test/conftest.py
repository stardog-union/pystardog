import pytest
import stardog.content as content
import stardog.content_types as content_types
import os

STARDOG_HOSTNAME_NODE_1 = os.environ['STARDOG_HOSTNAME_NODE_1']
STARDOG_HOSTNAME_CACHE = os.environ['STARDOG_HOSTNAME_CACHE']
STARDOG_HOSTNAME_STANDBY = os.environ['STARDOG_HOSTNAME_STANDBY']

def pytest_addoption(parser):
    parser.addoption("--username", action="store", default="admin")
    parser.addoption("--passwd", action="store", default="admin")
    parser.addoption("--endpoint", action="store", default="http://localhost:5820")

@pytest.fixture
def conn_string(pytestconfig):
    conn = {
        'endpoint': pytestconfig.getoption("endpoint"),
        'username': pytestconfig.getoption("username"),
        'password': pytestconfig.getoption("passwd")
    }
    return conn


@pytest.fixture
def bulkload_content():
    contents = [
        content.Raw(
            '<urn:subj> <urn:pred> <urn:obj3> .',
            content_types.TURTLE,
            name='bulkload.ttl'),
        (content.File('test/data/example.ttl.zip'), 'urn:context'),
        content.URL('https://www.w3.org/2000/10/rdf-tests/'
                    'RDF-Model-Syntax_1.0/ms_4.1_1.rdf')
    ]
    return contents


@pytest.fixture
def cache_target_info():
    target_info = {
        'target_name': 'pystardog-test-cache-target',
        'hostname': STARDOG_HOSTNAME_CACHE,
        'port': 5820,
        'username': 'admin',
        'password': 'admin'
    }
    return target_info

@pytest.fixture
def cluster_standby_node_conn_string():
    standby_conn_string= {
        'endpoint': f"http://{STARDOG_HOSTNAME_STANDBY}:5820",
        'username': 'admin',
        'password': 'admin'
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
        "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?useSSL=false"
    }
    return options

@pytest.fixture
def videos_options():
    options = {
        "jdbc.driver": "com.mysql.jdbc.Driver",
        "jdbc.username": "user",
        "jdbc.password": "pass",
        "mappings.syntax": "STARDOG",
        "jdbc.url": "jdbc:mysql://pystardog_mysql_videos/videos?useSSL=false"
    }
    return options
