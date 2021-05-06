import pytest

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
