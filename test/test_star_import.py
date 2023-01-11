from stardog import *

# Not recommended, but should still work
def test_starred_imports(conn_string):
    with Admin(**conn_string) as admin:
        assert admin.healthcheck()
