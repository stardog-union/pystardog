import pytest
import requests
import requests_mock

import stardog.exceptions
from stardog import content
from stardog.content_types import *


class TestMaterializeGraph:
    def test_materialize_graph_from_file_with_ds(self):
        from unittest.mock import patch, mock_open

        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            with requests_mock.Mocker() as m:
                m.post(
                    "http://localhost:5820/admin/virtual_graphs/import_db",
                    status_code=204,
                )

                admin = stardog.admin.Admin("http://localhost:5820", "admin", "admin")

                admin.materialize_virtual_graph(
                    "db_test",
                    stardog.content.MappingFile("test.sms2"),
                    "ds_test",
                    None,
                    None,
                )

    def test_materialize_graph_from_file_with_bad_ds(self):

        from unittest.mock import patch, mock_open

        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            with requests_mock.Mocker() as m:
                m.post(
                    "http://localhost:5820/admin/virtual_graphs/import_db",
                    status_code=404,
                    text="Data Source 'ds_sd_int_test' Not Found!",
                )

                admin = stardog.admin.Admin("http://localhost:5820", "admin", "admin")

                try:
                    admin.materialize_virtual_graph(
                        "db_test",
                        stardog.content.MappingFile("test.sms2"),
                        "ds_test",
                        None,
                        None,
                    )
                except stardog.exceptions.StardogException as e:
                    if e.http_code == 404:
                        assert True
                        return

        assert False

    def test_materialize_graph_payload(self):
        def text_callback(request, context):
            assert (
                request.text
                == '{"db": "db_test", "mappings": "data", "named_graph": null, "remove_all": false, "options": {"mappings.syntax": "SMS2"}, "data_source": "ds_test"}'
            )
            assert request.path == "/admin/virtual_graphs/import_db"
            return "response"

        from unittest.mock import patch, mock_open

        with patch("builtins.open", mock_open(read_data="data")) as mock_file:
            with requests_mock.Mocker() as m:
                m.post(
                    "http://localhost:5820/admin/virtual_graphs/import_db",
                    status_code=200,
                    text=text_callback,
                )

                admin = stardog.admin.Admin("http://localhost:5820", "admin", "admin")

                admin.materialize_virtual_graph(
                    "db_test",
                    stardog.content.MappingFile("test.sms2"),
                    "ds_test",
                    None,
                    None,
                )

    def test_materialize_graph_missing_ds_or_options(self):
        admin = stardog.admin.Admin("http://localhost:5820", "admin", "admin")

        try:
            admin.materialize_virtual_graph(
                "db_test",
                stardog.content.MappingFile("test.sms2"),
            )
        except AssertionError as e:
            assert True
            return

        assert False


class TestContentType:
    def test_guess_rdf_format(self):
        assert guess_rdf_format("test.ttl") == (None, TURTLE)
        assert guess_rdf_format("test.rdf") == (None, RDF_XML)
        assert guess_rdf_format("test.rdfs") == (None, RDF_XML)
        assert guess_rdf_format("test.owl") == (None, RDF_XML)
        assert guess_rdf_format("test.xml") == (None, RDF_XML)
        assert guess_rdf_format("test.nt") == (None, NTRIPLES)
        assert guess_rdf_format("test.n3") == (None, N3)
        assert guess_rdf_format("test.nq") == (None, NQUADS)
        assert guess_rdf_format("test.nquads") == (None, NQUADS)
        assert guess_rdf_format("test.trig") == (None, TRIG)
        assert guess_rdf_format("test.trix") == (None, TRIX)
        assert guess_rdf_format("test.json") == (None, LD_JSON)
        assert guess_rdf_format("test.jsonld") == (None, LD_JSON)

        assert guess_rdf_format("test.ttl.gz") == ("gzip", TURTLE)
        assert guess_rdf_format("test.ttl.zip") == ("zip", TURTLE)
        assert guess_rdf_format("test.ttl.bz2") == ("bzip2", TURTLE)

    def test_guess_mapping_format_from_filename(self):
        assert guess_mapping_format("test.rq") == "SMS2"
        assert guess_mapping_format("test.sms2") == "SMS2"
        assert guess_mapping_format("test.sms") == "SMS2"
        assert guess_mapping_format("test.r2rml") == "R2RML"
        assert guess_mapping_format("test.what") is None

    def test_guess_mapping_format_from_content(self):
        assert guess_mapping_format_from_content("MAPPING\nFROM ") == "SMS2"
        assert guess_mapping_format_from_content("#A comment\nMAPPING FROM ") == "SMS2"

    def test_guess_import_format(self):
        assert guess_import_format("test.csv") == (None, "text/csv", "DELIMITED", ",")
        assert guess_import_format("test.tsv") == (
            None,
            "text/tab-separated-values",
            "DELIMITED",
            "\t",
        )
        assert guess_import_format("test.json") == (
            None,
            "application/json",
            "JSON",
            None,
        )
        assert guess_import_format("test.what") == (None, None, None, None)

        assert guess_import_format("test.csv.gz") == (
            "gzip",
            "text/csv",
            "DELIMITED",
            ",",
        )
        assert guess_import_format("test.csv.zip") == (
            "zip",
            "text/csv",
            "DELIMITED",
            ",",
        )
        assert guess_import_format("test.csv.bz2") == (
            "bzip2",
            "text/csv",
            "DELIMITED",
            ",",
        )


class TestContent:
    def test_file(self):
        m = content.File("test.ttl")
        assert m.content_type == TURTLE
        assert m.content_encoding is None
        assert m.fname == "test.ttl"

        m = content.File("test.rdf")
        assert m.content_type == RDF_XML
        assert m.content_encoding is None

        m = content.File("test.rdfs")
        assert m.content_type == RDF_XML
        assert m.content_encoding is None

        m = content.File("test.owl")
        assert m.content_type == RDF_XML
        assert m.content_encoding is None

        m = content.File("test.xml")
        assert m.content_type == RDF_XML
        assert m.content_encoding is None

        m = content.File("test.nt")
        assert m.content_type == NTRIPLES
        assert m.content_encoding is None

        m = content.File("test.n3")
        assert m.content_type == N3
        assert m.content_encoding is None

        m = content.File("test.nq")
        assert m.content_type == NQUADS
        assert m.content_encoding is None

        m = content.File("test.nquads")
        assert m.content_type == NQUADS

        assert m.content_encoding is None

        m = content.File("test.trig")
        assert m.content_type == TRIG
        assert m.content_encoding is None

        m = content.File("test.trix")
        assert m.content_type == TRIX
        assert m.content_encoding is None

        m = content.File("test.json")
        assert m.content_type == LD_JSON
        assert m.content_encoding is None

        m = content.File("test.jsonld")
        assert m.content_type == LD_JSON
        assert m.content_encoding is None

        m = content.File("test.turtle", content_type="text/turtle", name="overrideName")
        assert m.content_type == "text/turtle"
        assert m.content_encoding is None
        assert m.name == "overrideName"

        m = content.File("test.ttl.gz")
        assert m.content_type == "text/turtle"
        assert m.content_encoding == "gzip"

        m = content.File("test.ttl.zip")
        assert m.content_type == "text/turtle"
        assert m.content_encoding == "zip"

        m = content.File("test.ttl.bz2")
        assert m.content_type == "text/turtle"
        assert m.content_encoding == "bzip2"

    def test_file_backward_compability(self):
        m = content.File(fname="test.ttl.bz2")
        assert m.content_type == "text/turtle"
        assert m.content_encoding == "bzip2"

    def test_raw(self):
        m = content.Raw("data", name="test.ttl")
        assert m.content_type == "text/turtle"
        m = content.Raw("data", name="test.rdf")
        assert m.content_type == "application/rdf+xml"
        m = content.Raw("data", name="test.rdfs")
        assert m.content_type == "application/rdf+xml"
        m = content.Raw("data", name="test.owl")
        assert m.content_type == "application/rdf+xml"
        m = content.Raw("data", name="test.xml")
        assert m.content_type == "application/rdf+xml"
        m = content.Raw("data", name="test.nt")
        assert m.content_type == "application/n-triples"
        m = content.Raw("data", name="test.n3")
        assert m.content_type == "text/n3"
        m = content.Raw("data", name="test.nq")
        assert m.content_type == "application/n-quads"
        m = content.Raw("data", name="test.nquads")
        assert m.content_type == "application/n-quads"
        m = content.Raw("data", name="test.trig")
        assert m.content_type == "application/trig"
        m = content.Raw("data", name="test.trix")
        assert m.content_type == "application/trix"
        m = content.Raw("data", name="test.json")
        assert m.content_type == "application/ld+json"
        m = content.Raw("data", name="test.jsonld")
        assert m.content_type == "application/ld+json"

    def test_mapping_file(self):
        m = content.MappingFile("test.sms")
        assert m.syntax == "SMS2"

        m = content.MappingFile("test.sms2")
        assert m.syntax == "SMS2"

        m = content.MappingFile("test.rq")
        assert m.syntax == "SMS2"

        m = content.MappingFile("test.r2rml")
        assert m.syntax == "R2RML"

        m = content.MappingFile("test.ttl")
        assert m.syntax is None
        assert m.name == "test.ttl"

        m = content.MappingFile("test.ttl", "SMS2", "overrideName")
        assert m.syntax == "SMS2"
        assert m.name == "overrideName"

    def test_mapping_raw(self):
        with open("data/test_import_delimited.sms") as f:
            m = content.MappingRaw(f.read())
            assert m.syntax == "SMS2"

        with open("data/r2rml.ttl") as f:
            m = content.MappingRaw(f.read())
            assert m.syntax is None

        m = content.MappingRaw("does not matter", name="mapping.sms2")
        assert m.syntax == "SMS2"
        assert m.name == "mapping.sms2"

        m = content.MappingRaw("does not matter", name="mapping.sms")
        assert m.syntax == "SMS2"

        m = content.MappingRaw("does not matter", name="mapping.rq")
        assert m.syntax == "SMS2"

        m = content.MappingRaw("does not matter", name="mapping.r2rml")
        assert m.syntax == "R2RML"

        m = content.MappingRaw("does not matter", name="mapping.ttl")
        assert m.syntax is None

    def test_import_file(self):
        # detect all values from filename for CSV
        m = content.ImportFile("test.csv")
        assert m.content_type == "text/csv"
        assert m.content_encoding is None
        assert m.input_type == "DELIMITED"
        assert m.name == "test.csv"
        assert m.separator == ","

        # detect all values from filename for TSV
        m = content.ImportFile("test.tsv")
        assert m.content_type == "text/tab-separated-values"
        assert m.content_encoding is None
        assert m.input_type == "DELIMITED"
        assert m.name == "test.tsv"
        assert m.separator == "\t"

        # detect all values from filename for JSON
        m = content.ImportFile("test.json")
        assert m.content_type == "application/json"
        assert m.content_encoding is None
        assert m.input_type == "JSON"
        assert m.name == "test.json"
        assert m.separator is None

        # SUPPORT custom DELIMITED format
        m = content.ImportFile(
            "test.delimited",
            content_type="text/delimited",
            input_type="DELIMITED",
            name="Override.myformat",
            separator=":",
        )
        assert m.content_type == "text/delimited"
        assert m.content_encoding is None
        assert m.input_type == "DELIMITED"
        assert m.name == "Override.myformat"
        assert m.separator == ":"

        m = content.ImportFile("test.csv.zip")
        assert m.content_encoding == "zip"

        m = content.ImportFile("test.csv.gz")
        assert m.content_encoding == "gzip"

        m = content.ImportFile("test.csv.bz2")
        assert m.content_encoding == "bzip2"

        m = content.ImportFile("test.csv.compress", content_encoding="compress")
        assert (m.separator, ",")
        assert m.content_encoding == "compress"

    def test_import_raw(self):
        # detect all values from name for CSV
        m = content.ImportRaw("data", name="test.csv")
        assert m.content_type == "text/csv"
        assert m.content_encoding is None
        assert m.input_type == "DELIMITED"
        assert m.name == "test.csv"
        assert m.separator == ","

        # detect all values from name for TSV
        m = content.ImportRaw("data", name="test.tsv")
        assert m.content_type == "text/tab-separated-values"
        assert m.content_encoding is None
        assert m.input_type == "DELIMITED"
        assert m.name == "test.tsv"
        assert m.separator == "\t"

        # detect all values from name for JSON
        m = content.ImportRaw("data", name="test.json")
        assert m.content_type == "application/json"
        assert m.content_encoding is None
        assert m.input_type == "JSON"
        assert m.name == "test.json"
        assert m.separator is None

        # detect all values from name for JSON
        m = content.ImportRaw("data", name="test.what")
        assert m.content_type is None
        assert m.content_encoding is None
        assert m.input_type is None
        assert m.name == "test.what"
        assert m.separator is None


class TestStardogException:
    def test_exception_orig(self):
        # While not appropriate raise StardogException from scratch, let's check if it still works the old way
        exception = stardog.exceptions.StardogException("Mymessage")
        assert str(exception) == "Mymessage"

    def test_exception(self):
        exception = stardog.exceptions.StardogException("Mymessage", 400, "SD90A")
        assert str(exception) == "Mymessage"
        assert exception.http_code == 400
        assert exception.stardog_code == "SD90A"
