import threading
import time

import pytest
from stardog import connection, content, content_types, exceptions


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


def test_processes(admin):
    # Do not assert this list is empty. The server runs its own managed
    # processes (for example on the catalog database), so the count is not
    # ours to control. test_processes_while_running covers the case where a
    # specific process is expected, and filters by database to find it.
    assert isinstance(admin.processes(), list)

    with pytest.raises(exceptions.StardogException, match="Process not found: 1"):
        admin.process(1)

    with pytest.raises(exceptions.StardogException, match="Process not found: 1"):
        admin.kill_process(1)


@pytest.mark.dbname("pystardog-test-database")
def test_processes_while_running(admin, conn_string, db):
    seed = "@prefix ex: <http://example.com/> .\n" + "\n".join(
        f"ex:s{i} ex:p ex:o{i} ." for i in range(1, 80)
    )
    update = """
    prefix ex: <http://example.com/>

    insert {
      ?s1 ex:derived ?o3 .
    }
    where {
      ?s1 ?p1 ?o1 .
      ?s2 ?p2 ?o2 .
      ?s3 ?p3 ?o3 .
    }
    """

    with connection.Connection(db.name, **conn_string) as conn:
        conn.begin()
        conn.clear()
        conn.add(content.Raw(seed, content_types.TURTLE))
        conn.commit()

    update_done = threading.Event()
    update_errors = []

    def run_update():
        try:
            with connection.Connection(db.name, **conn_string) as conn:
                conn.update(update)
        except exceptions.StardogException as exc:
            update_errors.append(exc)
        finally:
            update_done.set()

    worker = threading.Thread(target=run_update)
    worker.start()

    process = None
    deadline = time.time() + 10
    while time.time() < deadline and process is None:
        processes = admin.processes()
        for candidate in processes:
            if candidate.get("db") != db.name:
                continue
            if candidate.get("type") == "Transaction":
                process = candidate
                break
            if process is None:
                process = candidate
        if process is None:
            time.sleep(0.2)

    assert process is not None
    assert process["status"] == "RUNNING"

    details = admin.process(process["id"])
    assert details["id"] == process["id"]
    assert details["db"] == db.name
    assert details["status"] == "RUNNING"

    admin.kill_process(process["id"])

    worker.join(timeout=10)
    assert update_done.is_set()
    assert update_errors
    assert "cancel" in str(update_errors[0]).lower()


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
