import stardog.content as content
import stardog.content_types as content_types


def test_content():

    r = content.Raw('content', content_types.TURTLE, 'zip', 'raw.ttl.zip')
    with r.data() as c:
        assert c == 'content'
        assert r.content_type == content_types.TURTLE
        assert r.content_encoding == 'zip'
        assert r.name == 'raw.ttl.zip'

    f = content.File('test/data/example.ttl')
    with f.data() as c:
        assert c.read() == b'<urn:subj> <urn:pred> <urn:obj> .'
        assert f.content_type == content_types.TURTLE
        assert f.content_encoding is None
        assert f.name == 'example.ttl'

    f = content.File('test/data/example.ttl.zip')
    assert f.content_type == content_types.TURTLE
    assert f.content_encoding == 'zip'
    assert f.name == 'example.ttl.zip'

    u = content.URL('https://www.w3.org/2000/10/rdf-tests/'
                    'RDF-Model-Syntax_1.0/ms_4.1_1.rdf')
    with u.data() as c:
        assert c == open('test/data/ms_4.1_1.rdf', 'rb').read()
        assert u.content_type == content_types.RDF_XML
        assert u.name == 'ms_4.1_1.rdf'
