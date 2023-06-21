import os
import re
from enum import Enum

import pytest
from stardog import admin, connection, content, content_types, exceptions

###############################################################
#
# These test can be run against a cluster or standalone server
#
###############################################################
from stardog.exceptions import StardogException

default_users = ["admin"]
default_roles = ["reader"]
default_namespace_count = 7


class Resource(Enum):
    DB = "db_sd_int_test"
    DS = "ds_sd_int_test"
    VG = "vg_sd_int_test"
    NG = "http://example.org/graph"


class TestStardog:
    def setup_method(self, test_method):
        """
        Before each test a fresh stardog admin and credential object will be provided, just in case it got corrupted.

        @rtype: None

        """

        if not os.path.isdir("data") and not os.path.islink("data"):
            os.symlink("test/data", "data")

    # This is too inefficient for running all tests. we don't want to clean up everything after every single test
    # we only want to clean up the resources we created.
    # also we should use fixtures for creating resources, so that we can create and clean up in the same test.
    # teardown must be managed by fixtures
    def teardown_method(self, test_method):
        """
        After each test this will destroy all resources on the instance.

        @rtype: None
        """

    # maybe these two can get merged together, as they do virtually the same.
    def expected_count(self, conn, expected=1, ng: str = "stardog:context:default"):
        q = conn.select(f"select * where {{ graph {ng} {{?s ?p ?o}} }}")
        return len(q["results"]["bindings"]) == expected

    def count_records(self, bd_name, conn_string):
        with connection.Connection(bd_name, **conn_string) as conn:
            graph_name = conn.select("select ?g { graph ?g {}}")["results"]["bindings"][
                0
            ]["g"]["value"]
            q = conn.select("SELECT * { GRAPH <" + graph_name + "> { ?s ?p ?o }}")
            count = len(q["results"]["bindings"])
        return count

    @property
    def ng(self) -> str:
        """
        This method return the default named-graph string

        @return: str
        """
        return Resource.NG.value

    ################################################################################################################
    #
    # Database helpers
    #
    ################################################################################################################

    # This is redefined too many time. Need to choose one, and stick to that one (as a fixture, and as a private method in the database class)
    @property
    def music_options(self):
        return {
            "jdbc.driver": "com.mysql.jdbc.Driver",
            "jdbc.username": "user",
            "jdbc.password": "pass",
            "mappings.syntax": "STARDOG",
            "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?useSSL=false",
        }


class TestUserImpersonation(TestStardog):
    def test_impersonating_user_databases_visibility(self, conn_string, user, db):

        with admin.Admin(
            endpoint=conn_string["endpoint"],
        ) as admin_user:
            databases_admin_can_see = [db.name for db in admin_user.databases()]
        with admin.Admin(
            endpoint=conn_string["endpoint"],
            run_as=user.name,
        ) as admin_impersonating_user:
            databases_impersonated_user_can_see = [
                db.name for db in admin_impersonating_user.databases()
            ]
        assert len(databases_impersonated_user_can_see) == 0

        # for cluster tests in Circle, catalog is disabled so the exact number of dbs
        # varies (2 for single node, 1 for cluster since catalog isn't created)
        assert len(databases_admin_can_see) > 0
        assert db.name in databases_admin_can_see


class TestUsers(TestStardog):
    def test_user_creation(self, admin, user):
        assert len(admin.users()) == len(default_users) + 1
        assert not user.is_superuser()
        assert user.is_enabled()

    @pytest.mark.user_username("userCanChangePass")
    @pytest.mark.user_password("userCanChangePass")
    def test_user_can_change_password(self, conn_string, user):
        user.set_password("new_password")
        with admin.Admin(
            endpoint=conn_string["endpoint"],
            username="userCanChangePass",
            password="new_password",
        ) as admin_as_user:
            assert admin_as_user.validate()

    @pytest.mark.user_username("userCanValidate")
    @pytest.mark.user_password("userCanValidate")
    def test_new_user_can_connect(self, conn_string, user):
        with admin.Admin(
            endpoint=conn_string["endpoint"],
            username="userCanValidate",
            password="userCanValidate",
        ) as admin_as_user:
            assert admin_as_user.validate()

    def test_disable_enable_user(self, user):
        user.set_enabled(False)
        assert not user.is_enabled()
        user.set_enabled(True)
        assert user.is_enabled()

    def test_user_roles(self, user):
        assert len(user.roles()) == 0

        user.add_role("reader")
        roles = user.roles()
        assert len(user.roles()) == 1

        user.set_roles(*roles)
        assert len(user.roles()) == 1

        user.remove_role("reader")
        assert len(user.roles()) == 0

    @pytest.mark.user_username("testUserPermissions")
    @pytest.mark.user_password("testUserPermissions")
    def test_user_permissions(self, user):
        assert user.permissions() == [
            {
                "action": "WRITE",
                "resource_type": "user",
                "resource": ["testUserPermissions"],
            },
            {
                "action": "READ",
                "resource_type": "user",
                "resource": ["testUserPermissions"],
            },
        ]
        assert user.effective_permissions() == [
            {
                "action": "WRITE",
                "resource_type": "user",
                "resource": ["testUserPermissions"],
            },
            {
                "action": "READ",
                "resource_type": "user",
                "resource": ["testUserPermissions"],
            },
        ]

    def test_user_exists_in_user_list(self, admin, user):
        all_users = admin.users()
        assert user in all_users

    def test_non_existent_user_should_not_get_a_handle(self, admin):
        with pytest.raises(exceptions.StardogException, match="User .* does not exist"):
            admin.user("not a real user")


class TestRoles(TestStardog):
    def test_role_creation(self, admin, role):
        assert len(admin.roles()) == len(default_roles) + 1

    def test_role_permissions_empty(self, role):
        assert role.permissions() == []

    def test_role_add_and_remove_permission(self, role):
        role.add_permission("WRITE", "*", "*")
        assert role.permissions() == [
            {"action": "WRITE", "resource_type": "*", "resource": ["*"]}
        ]

        role.remove_permission("WRITE", "*", "*")
        assert role.permissions() == []

    def test_role_exists_in_role_list(self, admin, role):
        all_roles = admin.roles()
        assert role in all_roles

    def test_non_existing_role_should_not_get_a_handle(self, admin):
        with pytest.raises(exceptions.StardogException, match="Role .* does not exist"):
            admin.role("not a real role")


class TestDatabase(TestStardog):
    def _options():
        return {"search.enabled": True, "spatial.enabled": True}

    def _bld() -> list:
        contents = [
            content.Raw(
                "<urn:subj> <urn:pred> <urn:obj3> .",
                content_types.TURTLE,
                name="bulkload.ttl",
            ),
            (content.File("data/example.ttl.zip"), "urn:context"),
            content.URL(
                "https://www.w3.org/2000/10/rdf-tests/"
                "RDF-Model-Syntax_1.0/ms_4.1_1.rdf"
            ),
        ]
        return contents

    @pytest.mark.dbname("pystardog-db-name")
    def test_create_db(self, db):
        assert db.name == "pystardog-db-name"

    def test_default_db_properties(self, db):
        assert db.get_options("search.enabled", "spatial.enabled") == {
            "search.enabled": False,
            "spatial.enabled": False,
        }

    @pytest.mark.options(_options())
    def test_new_database_with_properties(self, db):
        assert db.get_options("search.enabled", "spatial.enabled") == {
            "search.enabled": True,
            "spatial.enabled": True,
        }

    def test_online_offline(self, db):
        # change options
        assert db.get_options("database.online") == {"database.online": True}
        db.offline()
        assert db.get_options("database.online") == {"database.online": False}
        db.online()
        assert db.get_options("database.online") == {"database.online": True}

    def test_get_all_options(self, db):
        options = db.get_all_options()
        assert len(options.keys()) > 150

    def test_optimized(self, db):
        db.optimize()
        # for now this is the best we can do
        assert True

    def test_verity(self, db):
        db.verify()
        # for now this is the best we can do
        assert True

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    # use of BULKLOAD, need to choose which one to leave as final.
    @pytest.mark.contents(_bld())
    @pytest.mark.kwargs({"copy_to_server": True})
    def test_bulkload(self, db, conn):
        assert self.expected_count(conn, 6)
        assert self.expected_count(conn, 1, ng="<urn:context>")

    def test_database_exists_in_databases_list(self, db, admin):
        all_databases = admin.databases()
        assert db in all_databases

    def test_non_existent_db_should_not_get_a_handle(self, admin):
        with pytest.raises(StardogException, match="does not exist"):
            admin.database("not_real_db")


# A namespace fixture creates a database, and assigns the namespace to that database.
# both gets cleaned up as part of the fixture.
class TestNamespaces(TestStardog):
    def test_add_and_delete_namespaces(self, db):
        assert len(db.namespaces()) == default_namespace_count

        db.add_namespace("testns", "my:test:IRI")
        assert len(db.namespaces()) == default_namespace_count + 1

        # tests a failure while adding an existing namespace
        with pytest.raises(
            Exception, match="Namespace already exists for this database"
        ):
            db.add_namespace("stardog", "someiri")

        db.remove_namespace("testns")
        assert len(db.namespaces()) == default_namespace_count

        # tests a failure while removing an existing namespace
        with pytest.raises(
            Exception, match="Namespace does not exists for this database"
        ):
            db.remove_namespace("non-existent-ns")

        # tests insertion of a pair of namespaces that is a substring of the first
        db.add_namespace("testnspace", "my:test:IRI")
        db.add_namespace("testns", "my:test:IRI")

        assert len(db.namespaces()) == default_namespace_count + 2

        # tests removal of the correct namespace, even if a similar namespace exists
        db.remove_namespace("testns")
        db.remove_namespace("testnspace")

        assert len(db.namespaces()) == default_namespace_count

    def test_import_namespaces(self, db):
        # we want to tests more than 1 file format
        namespaces_ttl = content.File("test/data/namespaces.ttl")
        namespaces_rdf = content.File("test/data/namespaces.xml")

        db_default_namespaces = db.namespaces()
        ns_default_count = len(db_default_namespaces)

        # number of namespaces is a fixed number set to default_namespace_count by default for any new database
        # https://docs.stardog.com/operating-stardog/database-administration/managing-databases#namespaces
        assert ns_default_count == default_namespace_count

        # imports 4 namespaces
        db.import_namespaces(namespaces_ttl)
        ttl_ns_count = len(db.namespaces())
        assert ns_default_count + 4 == ttl_ns_count

        # imports 2 namespaces
        db.import_namespaces(namespaces_rdf)
        rdf_ns_count = len(db.namespaces())
        assert ttl_ns_count + 2 == rdf_ns_count


class TestDataSource(TestStardog):
    @pytest.mark.ds_name("pystardog-test-datasource")
    def test_datasource_creation(self, admin, datasource):
        ds = admin.datasource("pystardog-test-datasource")
        assert ds.name == "pystardog-test-datasource"

    def test_datasource_exists_in_datasource_list(self, admin, datasource):
        all_datasources = admin.datasources()
        assert datasource in all_datasources

    def test_non_existent_datasource_should_not_get_a_handle(self, admin):
        with pytest.raises(
            exceptions.StardogException, match="There is no data source with name"
        ):
            admin.datasource("not a real data source")

    def test_data_source_update(self, datasource):
        current_options = datasource.get_options()
        assert "new_option" not in current_options
        new_options = {
            "jdbc.driver": "com.mysql.jdbc.Driver",
            "jdbc.username": "user",
            "jdbc.password": "pass",
            "mappings.syntax": "STARDOG",
            "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?useSSL=false",
            "new_option": "new option",
        }
        datasource.update(new_options)
        new_options_from_ds = datasource.get_options()
        assert "new_option" in new_options_from_ds

    @pytest.mark.use_music_datasource(True)
    def test_data_source_update_force(self, datasource, virtual_graph):
        current_options = datasource.get_options()
        assert "new_option" not in current_options
        new_options = {
            "jdbc.driver": "com.mysql.jdbc.Driver",
            "jdbc.username": "user",
            "jdbc.password": "pass",
            "mappings.syntax": "STARDOG",
            "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?useSSL=false",
            "new_option": "new option",
        }
        with pytest.raises(
            exceptions.StardogException,
            match="The data source .* is in use by virtual graphs",
        ):
            datasource.update(new_options, force=False)

        datasource.update(new_options, force=True)
        new_options_from_ds = datasource.get_options()
        assert "new_option" in new_options_from_ds


class TestLoadData(TestStardog):
    def setup_class(self):
        self.run_vg_test = True

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_data_add_ttl_from_file(self, db, conn):
        conn.begin()
        conn.add(content.File("data/example.ttl"))
        conn.commit()
        assert self.expected_count(conn, 1)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_data_add_ttl_from_file_server_side(self, db, conn):
        conn.begin()
        conn.add(content.File("/tmp/example-remote.ttl"), server_side=True)
        conn.commit()
        assert self.expected_count(conn, 1)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_data_add_ttl_from_content(self, db, conn):
        with open("data/example.ttl") as f:
            conn.begin()
            conn.add(content.Raw(f.read(), name="example.ttl"))
            conn.commit()
        assert self.expected_count(conn, 1)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_data_add_ttl_from_file_ns(self, db, conn):
        conn.begin()
        conn.add(content.File("data/example.ttl"), graph_uri=self.ng)
        conn.commit()
        assert self.expected_count(conn, 1, ng=f"<{self.ng}>")

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_csv_from_file(self, admin, db, conn):
        admin.import_file(
            db.name,
            content.MappingFile("data/test_import_delimited.sms"),
            content.ImportFile("data/test_import_delimited.csv"),
        )
        assert self.expected_count(conn, 145961)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_from_file_with_delimiter(self, admin, db, conn):
        admin.import_file(
            db.name,
            content.MappingFile("data/test_import_delimited.sms"),
            content.ImportFile(
                "data/test_import_pipe.txt", input_type="DELIMITED", separator="|"
            ),
        )
        assert self.expected_count(conn, 145961)

    # nested withs can be merged into 1
    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_csv_from_content(self, admin, db, conn):
        with open("data/test_import_delimited.csv") as csv:
            with open("data/test_import_delimited.sms") as sms:
                admin.import_file(
                    db.name,
                    content.MappingRaw(sms.read()),
                    content.ImportRaw(csv.read(), name="data.csv"),
                )
                assert self.expected_count(conn, 145961)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_csv_from_file_ns(self, admin, db, conn):
        admin.import_file(
            db.name,
            content.MappingFile("data/test_import_delimited.sms"),
            content.ImportFile("data/test_import_delimited.csv"),
            None,
            self.ng,
        )
        assert self.expected_count(conn, 145961, ng=f"<{self.ng}>")

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_tsv_from_file(self, admin, db, conn):
        admin.import_file(
            db.name,
            content.MappingFile("data/test_import_delimited.sms"),
            content.ImportFile("data/test_import_delimited.tsv"),
        )
        assert self.expected_count(conn, 145961)

    # 2 nested with can be merged
    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_tsv_from_content(self, admin, db, conn):
        with open("data/test_import_delimited.tsv") as tsv:
            with open("data/test_import_delimited.sms") as sms:
                admin.import_file(
                    db.name,
                    content.MappingRaw(sms.read()),
                    content.ImportRaw(tsv.read(), name="data.tsv"),
                )
                assert self.expected_count(conn, 145961)

    # put the tsv data in a namedgraph
    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_tsv_from_file_ns(self, admin, db, conn):
        admin.import_file(
            db.name,
            content.MappingFile("data/test_import_delimited.sms"),
            content.ImportFile("data/test_import_delimited.tsv"),
            None,
            self.ng,
        )
        assert self.expected_count(conn, 145961, ng=f"<{self.ng}>")

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_json_from_file(self, admin, db, conn):
        admin.import_file(
            db.name,
            content.MappingFile("data/test_import_json.sms"),
            content.ImportFile("data/test_import.json"),
        )
        assert self.expected_count(conn, 223)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_json_from_contents(self, admin, db, conn):
        with open("data/test_import.json") as json:
            with open("data/test_import_json.sms") as sms:
                admin.import_file(
                    db.name,
                    content.MappingRaw(sms.read()),
                    content.ImportRaw(json.read(), name="data.json"),
                )
                assert self.expected_count(conn, 223)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_json_from_file_ns(self, admin, db, conn):
        admin.import_file(
            db.name,
            content.MappingFile("data/test_import_json.sms"),
            content.ImportFile("data/test_import.json"),
            None,
            "http://example.org/graph",
        )
        assert self.expected_count(conn, 223, ng=f"<{self.ng}>")

    ## MATERIALIZE AND VG LOAD SHOULD BE PART OF VG TEST

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_materialize_graph_from_file(self, admin, db, conn):
        if self.run_vg_test:
            admin.materialize_virtual_graph(
                db.name,
                content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                None,
                self.music_options,
            )
            assert self.expected_count(conn, 37)
        else:
            pytest.skip(self.msg_vg_test)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_materialize_graph_from_file(self, admin, db, conn):
        if self.run_vg_test:
            admin.materialize_virtual_graph(
                db.name,
                content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                None,
                self.music_options,
            )
            assert self.expected_count(conn, 37)
        else:
            pytest.skip(self.msg_vg_test)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_materialize_graph_from_file_with_ds(self, admin, db, conn, datasource):

        if self.run_vg_test:
            admin.materialize_virtual_graph(
                db.name,
                content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                datasource.name,
            )
            assert self.expected_count(conn, 37)
        else:
            pytest.skip(self.msg_vg_test)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_materialize_graph_from_content(self, admin, db, conn):
        if self.run_vg_test:
            with open("data/music_mappings.ttl") as f:
                admin.materialize_virtual_graph(
                    db.name,
                    content.MappingRaw(f.read(), "STARDOG"),
                    None,
                    self.music_options,
                )
            assert self.expected_count(conn, 37)
        else:
            pytest.skip(self.msg_vg_test)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_materialize_graph_from_file_in_ng(self, admin, db, conn):
        if self.run_vg_test:
            admin.materialize_virtual_graph(
                db.name,
                content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                None,
                self.music_options,
                self.ng,
            )
            assert self.expected_count(conn, 37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_materialize_graph_from_file_with_ds(self, admin, db, datasource, conn):
        if self.run_vg_test:
            admin.materialize_virtual_graph(
                db.name,
                content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                datasource.name,
                None,
                self.ng,
            )
            assert self.expected_count(conn, 37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_materialize_graph_from_content_with_ng(self, admin, db, conn):
        if self.run_vg_test:
            with open("data/music_mappings.ttl") as f:
                admin.materialize_virtual_graph(
                    db.name,
                    content.MappingRaw(f.read(), "STARDOG"),
                    None,
                    self.music_options,
                    self.ng,
                )
            assert self.expected_count(conn, 37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)

    @pytest.mark.dbname("pystardog-test-database")
    @pytest.mark.conn_dbname("pystardog-test-database")
    def test_import_db_deprecated(self, admin, db, conn):
        if self.run_vg_test:
            admin.import_virtual_graph(
                db.name,
                content.File("data/music_mappings.ttl"),
                self.ng,
                False,
                self.music_options,
            )
            assert self.expected_count(conn, 37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)


class TestVirtualGraph(TestStardog):
    # Also available as fixture.
    def _music_options():
        options = {
            "jdbc.driver": "com.mysql.jdbc.Driver",
            "jdbc.username": "user",
            "jdbc.password": "pass",
            "mappings.syntax": "STARDOG",
            "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?allowPublicKeyRetrieval=true&useSSL=false",
        }
        return options

    def _video_options():
        properties = {
            "jdbc.driver": "com.mysql.jdbc.Driver",
            "jdbc.username": "user",
            "jdbc.password": "pass",
            "mappings.syntax": "STARDOG",
            "jdbc.url": "jdbc:mysql://pystardog_mysql_videos/videos?allowPublicKeyRetrieval=true&useSSL=false",
        }
        return properties

    def _bad_options():
        bad_options = {
            "jdbc.driver": "com.mysql.jdbc.Driver",
            "jdbc.username": "non-existent",
            "jdbc.password": "non-existent",
            "jdbc.url": "jdbc:mysql://non-existent",
        }
        return bad_options

    def _simple_options():
        return {"mappings.syntax": "SMS2"}

    @pytest.mark.use_music_datasource(True)
    def test_vg_update(self, virtual_graph):
        assert "mappings.syntax" not in virtual_graph.options()
        virtual_graph.update(
            "new_name_vg", mappings="", options={"mappings.syntax": "SMS2"}
        )
        assert "mappings.syntax" in virtual_graph.options()
        assert virtual_graph.name == "new_name_vg"

    @pytest.mark.use_music_datasource(True)
    def test_vg_no_options(self, virtual_graph):
        # namespace is the "default" option, so passing no options will still generate a namespace option
        assert len(virtual_graph.options()) == 1

    @pytest.mark.use_music_datasource(True)
    @pytest.mark.database_name("some-database")
    def test_associate_vg_with_db(self, virtual_graph):
        assert "some-database" == virtual_graph.info()["database"]

    @pytest.mark.virtual_graph_options(_video_options())
    def test_create_vg_with_data_source_specified_in_options(self, virtual_graph):
        # vg = admin.new_virtual_graph("test_vg", mappings="", options=music_options)
        # namespace is a default option, so final option count will be 1 + number of options added
        assert len(virtual_graph.options()) > 1

    # can't remember what is this supposed to do
    @pytest.mark.use_music_datasource(True)
    @pytest.mark.virtual_graph_options(_music_options())
    def test_vg_mappings(self, virtual_graph):
        # default is STARDOG
        assert (
            "@prefix : <http://api.stardog.com/> ."
            == virtual_graph.mappings_string().decode("utf-8")[0:37]
        )

        # we test the first string of the entire response, as the rest of the response contains randomly generated
        # strings each time a mapping is generated, hence we can't know beforehand what to compare it to.
        # we assume that if the first line of the response is what we expect, the rest of the mappings are retrieved successfully as well.
        assert (
            "@prefix : <http://api.stardog.com/> ."
            == virtual_graph.mappings_string("R2RML").decode("utf-8")[0:37]
        )

    @pytest.mark.use_music_datasource(True)
    @pytest.mark.mappings(content.File("test/data/music_mappings.ttl"))
    def test_create_vg_with_custom_mappings(self, virtual_graph):
        assert (
            "PREFIX : <http://stardog.com/tutorial/>"
            == virtual_graph.mappings_string().decode("utf-8")[0:39]
        )

    # this might be deprecated later, but we test it until then.
    @pytest.mark.use_music_datasource(True)
    def test_mappings_old(admin, virtual_graph):
        assert (
            "@prefix : <http://api.stardog.com/> ."
            == virtual_graph.mappings().decode("utf-8")[0:37]
        )

    @pytest.mark.virtual_graph_options(_bad_options())
    @pytest.mark.use_music_datasource(True)
    @pytest.mark.dbname("test-vg")
    @pytest.mark.conn_dbname("test-vg")
    @pytest.mark.vgname("test-vg")
    def test_datasource_preffered_over_options_for_vg_creation(
        self, db, virtual_graph, conn
    ):
        res = conn.select("SELECT * {GRAPH <virtual://test-vg> { ?s ?p ?o }} LIMIT 1")
        assert (
            "http://api.stardog.com/Artist/id=1"
            == res["results"]["bindings"][0]["s"]["value"]
        )

    @pytest.mark.virtual_graph_options(_simple_options())
    @pytest.mark.use_music_datasource(True)
    def test_extra_options_should_be_passed_to_vg(self, virtual_graph):
        assert "mappings.syntax" in virtual_graph.options()

    def test_should_fail_when_no_datasource_is_passed(self, admin):
        with pytest.raises(
            exceptions.StardogException, match="Unable to determine data source type"
        ):
            admin.new_virtual_graph("vg", content.File("test/data/r2rml.ttl"))

    def test_should_fail_if_vg_does_not_exists(self, admin):
        with pytest.raises(
            exceptions.StardogException, match="Virtual Graph non-existent Not Found!"
        ):
            vg = admin.virtual_graph("non-existent")
            vg.available()

    @pytest.mark.skip(
        reason="We need to get sorted whether we want users to deal with prefix:// for vg/ds operations"
    )
    def test_vg_exists_in_vg_list(self, admin, virtual_graph_music):
        all_vgs = admin.virtual_graphs()
        assert virtual_graph_music in all_vgs

    @pytest.mark.skip(reason="Fix me later")
    # music options is passed as a fixture, need to make sure whether this is going to be a fixture or not.
    @pytest.mark.dbname("test-import-db")
    def test_import_vg(self, admin, db, music_options):
        graph_name = "test-graph"

        # tests passing mappings
        admin.import_virtual_graph(
            "test-import-db",
            mappings=content.File("test/data/music_mappings.ttl"),
            named_graph=graph_name,
            remove_all=True,
            options=music_options,
        )
        # specified mapping file generates a graph with total of 37 triples
        assert 37 == self.count_records(db.name, conn_string)

        # tests passing empty mappings
        admin.import_virtual_graph(
            "test-import-db",
            mappings="",
            named_graph=graph_name,
            remove_all=True,
            options=music_options,
        )
        # if mapping is not specified, music bd generates a graph with 79 triples
        assert 79 == self.count_records(db.name, conn_string)

        # test removing_all false, it should return  music records + video records.
        admin.import_virtual_graph(
            "test-import-db",
            mappings="",
            named_graph=graph_name,
            remove_all=False,
            options=videos_options,
        )
        # if no mapping is specified, videos db generates a graph with 800 triples. adding un-mapped music sums up to 879.
        assert 879 == self.count_records(db.name, conn_string)


class TestStoredQueries(TestStardog):
    def test_query_does_not_exists(self, admin):
        with pytest.raises(exceptions.StardogException, match="Stored query not found"):
            admin.stored_query("not a real stored query")

    @pytest.mark.query_name("pystardog-stored-query-test")
    def test_add_stored_query(self, stored_query):
        stored_query.name == "pystardog-stored-query-test"

    def test_update_stored_query(self, stored_query):
        # update a stored query
        assert stored_query.description is None
        stored_query.update(description="get all the triples")
        assert stored_query.description == "get all the triples"

    def test_clear_all_stored_queries(self, admin):
        # We don't use fixture because we want to handle the cleanup ourselves here.
        admin.new_stored_query("everything", "select * where { ?s ?p ?o . }")
        admin.new_stored_query("everything2", "select * where { ?s ?p ?o . }")
        assert len(admin.stored_queries()) == 2
        admin.clear_stored_queries()
        assert len(admin.stored_queries()) == 0

    def test_query_in_query_list(self, admin, stored_query):
        assert stored_query.name in [sq.name for sq in admin.stored_queries()]

        # this was not in the test, for some reason don't work. need to confirm why some of these work and some wont'
        # assert stored_query.name in admin.stored_queries()
