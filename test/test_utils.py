import pytest

from stardog import content, content_types
from stardog.utils import validate_iri


@pytest.mark.parametrize(
    "iri", ["<urn:graph>", "urn:graph>", "urn: graph", "urn:gra\nph", "graph", ""]
)
def test_graph_uri_validation_rejects(iri):
    with pytest.raises(ValueError):
        validate_iri(iri)


@pytest.mark.parametrize("iri", ["urn:graph", "http://example.com/g", None])
def test_graph_uri_validation_accepts(iri):
    validate_iri(iri)


def test_content():

    r = content.Raw("content", content_types.TURTLE, "zip", "raw.ttl.zip")
    with r.data() as c:
        assert c == "content"
        assert r.content_type == content_types.TURTLE
        assert r.content_encoding == "zip"
        assert r.name == "raw.ttl.zip"

    f = content.File("test/data/example.ttl")
    with f.data() as c:
        assert c.read() == b"<urn:subj> <urn:pred> <urn:obj> ."
        assert f.content_type == content_types.TURTLE
        assert f.content_encoding is None
        assert f.name == "example.ttl"

    f = content.File("test/data/example.ttl.zip")
    assert f.content_type == content_types.TURTLE
    assert f.content_encoding == "zip"
    assert f.name == "example.ttl.zip"

    u = content.URL(
        "https://www.w3.org/2000/10/rdf-tests/" "RDF-Model-Syntax_1.0/ms_4.1_1.rdf"
    )
    with u.data() as c:
        assert c == open("test/data/ms_4.1_1.rdf", "rb").read()
        assert u.content_type == content_types.RDF_XML
        assert u.name == "ms_4.1_1.rdf"
