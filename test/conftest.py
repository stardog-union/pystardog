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


@pytest.fixture
def ds_options():
    options = {
        "namespaces": "stardog=tag:stardog:api",
        "jdbc.driver": "com.mysql.jdbc.Driver",
        "jdbc.username": "user",
        "jdbc.password": "pass",
        "jdbc.url": "jdbc:mysql://pystardog_mysql/music"
    }
    return options
