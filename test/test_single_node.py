import pytest
import os
import datetime
from stardog import admin, connection, content, exceptions


def test_database_repair(admin, bulkload_content):
    bl = admin.new_database("bulkload", {}, *bulkload_content)
    bl.offline()
    assert bl.repair()
    bl.drop()


def test_backup_and_restore(admin, conn_string):
    def check_db_for_contents(dbname, num_results):
        with connection.Connection(dbname, **conn_string) as c:
            q = c.select("select * where { ?s ?p ?o }")
            assert len(q["results"]["bindings"]) == num_results

    # determine the path to the backups
    now = datetime.datetime.now()
    date = now.strftime("%Y-%m-%d")
    # This only makes sense if stardog server is running in the same server. Recommended STARDOG_HOME
    # should be /var/opt/stardog which I am setting as default in case of hitting a remote server.
    stardog_home = os.getenv("STARDOG_HOME", "/var/opt/stardog")
    restore_from = os.path.join(f"{stardog_home}", ".backup", "backup_db", f"{date}")

    # make a db with test data loaded
    db = admin.new_database(
        "backup_db", {}, content.File("test/data/starwars.ttl"), copy_to_server=True
    )

    db.backup()
    db.drop()

    # data is back after restore
    try:
        admin.restore(from_path=restore_from)
    except Exception as e:
        raise Exception(str(e) + ". Check whether $STARDOG_HOME is set")
    check_db_for_contents("backup_db", 90)

    # error if attempting to restore over an existing db without force
    with pytest.raises(exceptions.StardogException, match="Database already exists"):
        admin.restore(from_path=restore_from)

    # restore to a new db
    admin.restore(from_path=restore_from, name="backup_db2")
    check_db_for_contents("backup_db2", 90)

    # force to overwrite existing
    db.drop()
    db = admin.new_database("backup_db")
    check_db_for_contents("backup_db", 0)
    admin.restore(from_path=restore_from, force=True)
    check_db_for_contents("backup_db", 90)

    # the backup location can be specified
    db.backup(to=os.path.join(stardog_home, "backuptest"))

    # clean up
    db.drop()
    admin.database("backup_db2").drop()
