import pytest

from stardog.admin import Admin
from stardog.connection import Connection
from stardog.content import Raw, File, URL
from stardog.content_types import TURTLE

@pytest.fixture(scope="module")
def conn():
    conn = Admin(username='admin', password='admin')

    for db in conn.databases():
        db.drop()

    return conn


def test_databases(conn):
    assert len(conn.databases()) == 0

    # bulk load
    contents = [
        Raw('<urn:subj> <urn:pred> <urn:obj3> .', TURTLE, 'bulkload.ttl'),
        (File('test/data/example.ttl'), '<urn:context>'),
        URL('https://www.w3.org/2000/10/rdf-tests/RDF-Model-Syntax_1.0/ms_4.1_1.rdf')
    ]

    bl = conn.new_database('bulkload', contents=contents)

    c = Connection('bulkload', username='admin', password='admin')
    assert c.size() == 7

    bl.drop()
