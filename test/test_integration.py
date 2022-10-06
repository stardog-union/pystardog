import copy
import os
import re
import sys
from enum import Enum

import pytest
import stardog
from stardog import admin, connection, content, content_types

###############################################################
#
# These test can be run against a cluster or standalone server
#
###############################################################
from stardog.exceptions import StardogException


class Resource(Enum):
    DB = "db_sd_int_test"
    DS = "ds_sd_int_test"
    VG = "vg_sd_int_test"
    NG = "http://example.org/graph"


class TestStardog:
    is_local = (
        True
        if "localhost" in os.environ.get("STARDOG_ENDPOINT", "http://localhost:5820")
        and not os.path.exists("/.dockerenv")
        else False
    )

    def setup_method(self, test_method):
        """
        Before each test a fresh stardog admin and credential object will be provided, just in case it got corrupted.

        @rtype: None

        """

        conn = {
            "endpoint": os.environ.get("STARDOG_ENDPOINT", "http://localhost:5820"),
            "username": os.environ.get("STARDOG_USERNAME", "admin"),
            "password": os.environ.get("STARDOG_PASSWORD", "admin"),
        }
        self.conn = conn
        self.admin = stardog.Admin(**conn)

        if not os.path.isdir("data") and not os.path.islink("data"):
            os.symlink("test/data", "data")

    def teardown_method(self, test_method):
        """
        After each test this will destroy all resources on the instance.

        @rtype: None
        """

        dbs = self.admin.databases()

        for db in dbs:
            try:
                db.drop()
            except StardogException as e:
                if e.stardog_code != "0D0DU2":
                    raise e
                pass

        vgs = self.admin.virtual_graphs()

        for vg in vgs:
            try:
                vg.delete()
            except StardogException as e:
                if e.stardog_code != "0D0DU2":
                    raise e
                pass

        dss = self.admin.datasources()

        for ds in dss:
            try:
                ds.delete()
            except StardogException as e:
                if e.stardog_code != "0D0DU2":
                    raise e
                pass

    def expected_count(
        self, expected=1, db: str = None, ng: str = "stardog:context:default"
    ) -> bool:
        db = db if db else self.db_name

        with connection.Connection(db, **self.conn) as c:
            x = f"select * where {{ graph {ng} {{?s ?p ?o}} }}"
            q = c.select(f"select * where {{ graph {ng} {{?s ?p ?o}} }}")
            return len(q["results"]["bindings"]) == expected

    def connection(self, db: str = None):
        db = db if db else self.db_name

        return connection.Connection(db, **self.conn)

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

    @property
    def db(self) -> stardog.admin.Database:
        """
        This method will return a new default database object for the test. If it exists it will be destroyed

        @rtype: stardog.admin.Database
        """
        db = self.admin.database(self.db_name)

        try:
            db.drop()
        except StardogException as e:
            if e.stardog_code != "0D0DU2":
                raise e
            pass

        return self.admin.new_database(self.db_name)

    @property
    def db_name(self) -> str:
        """
        This method will return the default database name

        @rtype: str
        """
        return Resource.DB.value

    @property
    def bulk_load_content(self) -> list:
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

    ################################################################################################################
    #
    # Datasource  & VirtualGraph helpers
    #
    ################################################################################################################
    @property
    def ds(self) -> stardog.admin.DataSource:
        """
        This method will return a new default datasource object for the test. If it exists it will be destroyed and recreated.

        @rtype: stardog.admin.DataSource
        """
        ds = self.admin.datasource(self.ds_name)

        try:
            ds.delete()
        except StardogException as e:
            if e.http_code != 404:
                raise e
            pass

        return self.admin.new_datasource(self.ds_name, self.music_options)

    @property
    def ds_name(self):
        """
        This method will return the default data-source name

        @rtype: str
        """
        return Resource.DS.value

    @property
    def vg(self) -> stardog.admin.VirtualGraph:
        """
        This method will return a new default virtual_graph object for the test. If it exists it will be destroyed and recreated.

        @rtype: stardog.admin.VirtualGraph
        """
        ds = self.admin.virtual_graph(self.db_name)

        try:
            ds.delete()
        except StardogException as e:
            if e.stardog_code != "0D0DU2":
                raise e
            pass

        return self.admin.new_datasource(self.ds_name)

    @property
    def vg_name(self):
        """
        This method will return the default virtual_graph name

        @rtype: str
        """
        return Resource.DS.value

    @property
    def music_options(self):
        if TestStardog.is_local:
            return {
                "jdbc.url": "jdbc:sqlite:/tmp/music.db",
                "jdbc.username": "whatever",
                "jdbc.password": "whatever",
                "jdbc.driver": "org.sqlite.JDBC",
                "sql.default.schema": "main",
                "sql.defaults": "main",
                "sql.skip.validation": "true",
                "sql.dialect": "POSTGRESQL",
            }
        else:
            return {
                "jdbc.driver": "com.mysql.jdbc.Driver",
                "jdbc.username": "user",
                "jdbc.password": "pass",
                "mappings.syntax": "STARDOG",
                "jdbc.url": "jdbc:mysql://pystardog_mysql_music/music?useSSL=false",
            }


class TestDatabase(TestStardog):
    def test_database_creation(self):
        # create database, validate it created and drop it.

        db = self.admin.new_database(self.db_name)
        assert len(self.admin.databases()) == 1
        assert db.name == self.db_name

        # check that the default are used
        assert db.get_options("search.enabled", "spatial.enabled") == {
            "search.enabled": False,
            "spatial.enabled": False,
        }

        db = self.admin.database(self.db_name)
        assert db.name == self.db_name

        db.drop()
        assert len(self.admin.databases()) == 0

    def test_new_with_properties(self):
        db = self.admin.new_database(
            self.db_name, {"search.enabled": True, "spatial.enabled": True}
        )
        assert db.get_options("search.enabled", "spatial.enabled") == {
            "search.enabled": True,
            "spatial.enabled": True,
        }

    def test_online_offline(self):
        db = self.db

        # change options
        assert db.get_options("database.online") == {"database.online": True}
        db.offline()
        assert db.get_options("database.online") == {"database.online": False}
        db.online()
        assert db.get_options("database.online") == {"database.online": True}

    def test_get_all_options(self):
        db = self.db

        options = db.get_all_options()

        assert len(options.keys()) > 150

    def test_optimized(self):
        db = self.db
        db.optimize()

        # for now this is the best we can do
        assert True

    def test_verity(self):
        db = self.db
        db.verify()

        # for now this is the best we can do
        assert True

    def test_bulkload(self):
        self.admin.new_database(
            self.db_name, {}, *self.bulk_load_content, copy_to_server=True
        )
        assert self.expected_count(6)
        assert self.expected_count(1, ng="<urn:context>")


class TestDataSource(TestStardog):
    def test_datasource_creation(self):
        # create datasource, validate it created, and delete it
        ds = self.admin.new_datasource(self.ds_name, self.music_options)

        assert len(self.admin.datasources()) == 1
        assert ds.name == self.ds_name

        ds = self.admin.datasource(self.ds_name)
        assert ds.name == self.ds_name

        ds.delete()
        assert len(self.admin.datasources()) == 0


class TestLoadData(TestStardog):
    def setup_class(self):
        self.run_vg_test = True

        if TestStardog.is_local:
            # Let's check if we have the sqlite driver
            libpath = os.environ.get("STARDOG_EXT", None)

            driver_found = False
            if libpath:
                d = re.compile("sqlite-jdbc")
                for file in os.listdir(libpath):
                    if d.match(file):
                        driver_found = True

            if driver_found:
                if not os.path.exists("/tmp/music.db"):
                    import sqlite3
                    from sqlite3 import Error

                    conn = None
                    try:
                        conn = sqlite3.connect("/tmp/music.db")
                        with open("data/music_schema.sql") as f:
                            conn.executescript(f.read())
                        with open("data/beatles.sql") as f:
                            conn.executescript(f.read())
                    except Error as e:
                        self.run_vg_test = False
                    except FileNotFoundError as e:
                        self.run_vg_test = False
                    finally:
                        if conn:
                            conn.close()
            else:
                self.run_vg_test = False
                self.msg_vg_test = """
No sqlite driver detected, all virtual graph test will be disabled
Download driver from https://search.maven.org/artifact/org.xerial/sqlite-jdbc
And install in directory pointed to by STARDOG_EXT and restart server
"""

    def test_data_add_ttl_from_file(self):
        db = self.db
        with self.connection() as c:
            c.begin()
            c.add(stardog.content.File("data/example.ttl"))
            c.commit()
        assert self.expected_count(1)

    def test_data_add_ttl_from_content(self):
        db = self.db
        with self.connection() as c:
            with open("data/example.ttl") as f:
                c.begin()
                c.add(stardog.content.Raw(f.read(), name="example.ttl"))
                c.commit()
        assert self.expected_count(1)

    # can we put the ttl data in a namedgraph
    def test_data_add_ttl_from_file_ns(self):
        db = self.db
        with self.connection() as c:
            c.begin()
            c.add(stardog.content.File("data/example.ttl"), graph_uri=self.ng)
            c.commit()
        assert self.expected_count(1, ng=f"<{self.ng}>")

    def test_import_csv_from_file(self):
        db = self.db
        self.admin.import_file(
            db.name,
            stardog.content.MappingFile("data/test_import_delimited.sms"),
            stardog.content.ImportFile("data/test_import_delimited.csv"),
        )
        assert self.expected_count(145961)

    def test_import_csv_from_content(self):
        db = self.db
        with open("data/test_import_delimited.csv") as csv:
            with open("data/test_import_delimited.sms") as sms:
                self.admin.import_file(
                    db.name,
                    stardog.content.MappingRaw(sms.read()),
                    stardog.content.ImportRaw(csv.read(), name="data.csv"),
                )
                assert self.expected_count(145961)

    # put the csv data in a namedgraph
    def test_import_csv_from_file_ns(self):
        db = self.db
        self.admin.import_file(
            db.name,
            stardog.content.MappingFile("data/test_import_delimited.sms"),
            stardog.content.ImportFile("data/test_import_delimited.csv"),
            None,
            self.ng,
        )
        assert self.expected_count(145961, ng=f"<{self.ng}>")

    def test_import_tsv_from_file(self):
        db = self.db
        self.admin.import_file(
            db.name,
            stardog.content.MappingFile("data/test_import_delimited.sms"),
            stardog.content.ImportFile("data/test_import_delimited.csv"),
        )
        assert self.expected_count(145961)

    def test_import_tsv_from_content(self):
        db = self.db
        with open("data/test_import_delimited.csv") as tsv:
            with open("data/test_import_delimited.sms") as sms:
                self.admin.import_file(
                    db.name,
                    stardog.content.MappingRaw(sms.read()),
                    stardog.content.ImportRaw(tsv.read(), name="data.csv"),
                )
                assert self.expected_count(145961)

    # put the tsv data in a namedgraph
    def test_import_tsv_from_file_ns(self):
        db = self.db
        self.admin.import_file(
            db.name,
            stardog.content.MappingFile("data/test_import_delimited.sms"),
            stardog.content.ImportFile("data/test_import_delimited.tsv"),
            None,
            self.ng,
        )
        assert self.expected_count(145961, ng=f"<{self.ng}>")

    def test_import_json_from_file(self):
        db = self.db
        self.admin.import_file(
            db.name,
            stardog.content.MappingFile("data/test_import_json.sms"),
            stardog.content.ImportFile("data/test_import.json"),
        )
        assert self.expected_count(223)

    def test_import_json_from_contents(self):
        db = self.db
        with open("data/test_import.json") as json:
            with open("data/test_import_json.sms") as sms:
                self.admin.import_file(
                    db.name,
                    stardog.content.MappingRaw(sms.read()),
                    stardog.content.ImportRaw(json.read(), name="data.json"),
                )
                assert self.expected_count(223)

    def test_import_json_from_file_ns(self):
        db = self.db
        self.admin.import_file(
            db.name,
            stardog.content.MappingFile("data/test_import_json.sms"),
            stardog.content.ImportFile("data/test_import.json"),
            None,
            "http://example.org/graph",
        )
        assert self.expected_count(223, ng=f"<{self.ng}>")

    def test_materialize_graph_from_file(self):
        db = self.db

        if self.run_vg_test:
            self.admin.materialize_virtual_graph(
                db.name,
                stardog.content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                None,
                self.music_options,
            )
            assert self.expected_count(37)
        else:
            pytest.skip(self.msg_vg_test)

    def test_materialize_graph_from_file(self):
        db = self.db

        if self.run_vg_test:
            self.admin.materialize_virtual_graph(
                db.name,
                stardog.content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                None,
                self.music_options,
            )
            assert self.expected_count(37)
        else:
            pytest.skip(self.msg_vg_test)

    def test_materialize_graph_from_file_with_ds(self):
        db = self.db
        ds = self.ds

        if self.run_vg_test:
            self.admin.materialize_virtual_graph(
                db.name,
                stardog.content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                ds.name,
            )
            assert self.expected_count(37)
        else:
            pytest.skip(self.msg_vg_test)

    def test_materialize_graph_from_content(self):
        db = self.db

        if self.run_vg_test:
            with open("data/music_mappings.ttl") as f:
                self.admin.materialize_virtual_graph(
                    db.name,
                    stardog.content.MappingRaw(f.read(), "STARDOG"),
                    None,
                    self.music_options,
                )
            assert self.expected_count(37)
        else:
            pytest.skip(self.msg_vg_test)

    def test_materialize_graph_from_file_in_ng(self):
        db = self.db

        if self.run_vg_test:
            self.admin.materialize_virtual_graph(
                db.name,
                stardog.content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                None,
                self.music_options,
                self.ng,
            )
            assert self.expected_count(37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)

    def test_materialize_graph_from_file_with_ds(self):
        db = self.db
        ds = self.ds

        if self.run_vg_test:
            self.admin.materialize_virtual_graph(
                db.name,
                stardog.content.MappingFile("data/music_mappings.ttl", "STARDOG"),
                ds.name,
                None,
                self.ng,
            )
            assert self.expected_count(37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)

    def test_materialize_graph_from_content_with_ng(self):
        db = self.db

        if self.run_vg_test:
            with open("data/music_mappings.ttl") as f:
                self.admin.materialize_virtual_graph(
                    db.name,
                    stardog.content.MappingRaw(f.read(), "STARDOG"),
                    None,
                    self.music_options,
                    self.ng,
                )
            assert self.expected_count(37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)

    def test_import_db_deprecated(self):
        db = self.db

        if self.run_vg_test:
            self.admin.import_virtual_graph(
                db.name,
                stardog.content.File("data/music_mappings.ttl"),
                self.ng,
                False,
                self.music_options,
            )
            assert self.expected_count(37, ng=f"<{self.ng}>")
        else:
            pytest.skip(self.msg_vg_test)
