import pytest
from stardog import exceptions


def test_get_server_metrics(admin):
    assert "dbms.storage.levels" in admin.get_server_metrics()


def test_get_prometheus_metrics(admin):
    assert "TYPE databases_planCache_size gauge" in admin.get_prometheus_metrics()


def test_get_metadata_properties(admin):
    assert "database.archetypes" in admin.get_all_metadata_properties()


def test_alive(admin):
    assert admin.alive()


def test_healthcheck(admin):
    assert admin.healthcheck()


def test_queries(admin):
    assert len(admin.queries()) == 0

    with pytest.raises(exceptions.StardogException, match="Query not found: 1"):
        admin.query(1)

    with pytest.raises(exceptions.StardogException, match="Query not found: 1"):
        admin.kill_query(1)


## This might or might not be better to move it to a separate file.
## Since we move to machine executor, we don't really need to ssh, since we can modify the files on the host
@pytest.mark.skip(
    reason="We need to sort out how we are going to deal with ssh, since it's no longer required"
)
def test_backup_all(admin):
    admin.backup_all()

    default_backup = subprocess.run(
        [
            "sshpass",
            "-p",
            SSH_PASS,
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "ssh://" + SSH_USER + "@" + STARDOG_HOSTNAME_NODE_1 + ":2222",
            "--",
            "ls",
            "-la",
            "/var/opt/stardog/",
        ],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    assert ".backup" in default_backup.stdout

    admin.backup_all(location="/tmp")
    tmp_backup = subprocess.run(
        [
            "sshpass",
            "-p",
            SSH_PASS,
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "ssh://" + SSH_USER + "@" + STARDOG_HOSTNAME_NODE_1 + ":2222",
            "--",
            "ls",
            "-l",
            "/tmp",
        ],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    assert "meta" in tmp_backup.stdout
