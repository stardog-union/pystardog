import pytest
import stardog.admin


@pytest.fixture()
def admin(conn_string):
    with stardog.admin.Admin(**conn_string) as admin:
        yield admin


def test_database_repair(admin, bulkload_content):
    bl = admin.new_database('bulkload', {}, *bulkload_content)
    bl.offline()
    assert bl.repair()
    bl.drop()


