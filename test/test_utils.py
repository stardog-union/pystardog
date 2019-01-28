from stardog.content import URL, File, Raw
from stardog.content_types import RDF_XML, TURTLE


def test_content():

    r = Raw('content', TURTLE, 'zip', 'raw.ttl.zip')
    with r.data() as c:
        assert c == 'content'
        assert r.content_type == TURTLE
        assert r.content_encoding == 'zip'
        assert r.name == 'raw.ttl.zip'

    f = File('test/data/example.ttl')
    with f.data() as c:
        assert c.read() == b'<urn:subj> <urn:pred> <urn:obj> .'
        assert f.content_type == TURTLE
        assert f.content_encoding is None
        assert f.name == 'example.ttl'

    f = File('test/data/example.ttl.zip')
    assert f.content_type == TURTLE
    assert f.content_encoding == 'zip'
    assert f.name == 'example.ttl.zip'

    u = URL('https://www.w3.org/2000/10/rdf-tests/'
            'RDF-Model-Syntax_1.0/ms_4.1_1.rdf')
    with u.data() as c:
        assert c.read() == open('test/data/ms_4.1_1.rdf', 'rb').read()
        assert u.content_type == RDF_XML
        assert u.name == 'ms_4.1_1.rdf'
