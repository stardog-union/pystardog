import pytest
from stardog import admin as sd_admin
from stardog import exceptions


@pytest.mark.skip(
    reason="Implementation is not well documented, https://stardog.atlassian.net/browse/PLAT-2946"
)
def test_cluster_diagnostic_report(admin):
    admin.cluster_diagnostic_reports()


def test_cluster_readonly(admin):
    admin.cluster_start_readonly()
    with pytest.raises(exceptions.StardogException, match="The cluster is read only"):
        admin.new_database("fail_db")
    admin.cluster_stop_readonly()


def test_coordinator_check(admin, conn_string):
    coordinator_info = admin.cluster_info()["coordinator"]
    coordinator_conn_string = conn_string
    coordinator_conn_string["endpoint"] = "http://" + coordinator_info
    with sd_admin.Admin(**coordinator_conn_string) as admin_coordinator_check:
        assert admin_coordinator_check.cluster_coordinator_check()
