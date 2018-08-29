import pytest
import shutil

from stardog.http.client import Client
from stardog.connection import Connection
from stardog.admin import Admin
from stardog.content_types import TURTLE
from stardog.exceptions import StardogException
from stardog.content import Raw, File, URL

@pytest.fixture(scope="module")
def conn():
    return Connection('newtest', username='admin', password='admin')

@pytest.fixture(scope="module")
def admin():
    admin = Admin(username='admin', password='admin')

    for db in admin.databases():
        db.drop()
    
    admin.new_database('newtest', {'search.enabled': True, 'versioning.enabled': True})
    
    return admin

def test_transactions(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> .'

    # add
    conn.begin()
    conn.add(Raw(data, TURTLE))
    conn.commit()

    assert conn.size() == 1

    # remove
    conn.begin()
    conn.remove(File('test/data/example.ttl'))
    conn.commit()

    assert conn.size() == 0

    # rollback
    conn.begin()
    conn.add(Raw(data, TURTLE))
    conn.rollback()

    assert conn.size() == 0

    # export
    conn.begin()
    conn.add(URL('https://www.w3.org/2000/10/rdf-tests/RDF-Model-Syntax_1.0/ms_4.1_1.rdf'))
    conn.commit()

    assert '<http://description.org/schema/attributedTo>' in conn.export()

    with conn.export(stream=True, chunk_size=1) as stream:
        assert '<http://description.org/schema/attributedTo>' in ''.join(stream)

    # clear
    conn.begin()
    conn.clear()
    conn.commit()

    assert conn.size() == 0

def test_queries(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    # add
    conn.begin()
    conn.add(Raw(data, TURTLE))
    conn.commit()

    # select
    q = conn.select('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 2

    # params
    q = conn.select('select * {?s ?p ?o}', offset=1, limit=1)
    assert len(q['results']['bindings']) == 1

    # bindings
    q = conn.select('select * {?s ?p ?o}', bindings={'o': '<urn:obj>'})
    assert len(q['results']['bindings']) == 1

    # paths
    q = conn.paths('paths start ?x = <urn:subj> end ?y = <urn:obj> via ?p')
    assert len(q['results']['bindings']) == 1

    # ask
    q = conn.ask('ask {<urn:subj> <urn:pred> <urn:obj>}')
    assert q == True

    # construct
    q = conn.graph('construct {?s ?p ?o} where {?s ?p ?o}')
    assert q.strip() == data

    # update
    q = conn.update('delete where {?s ?p ?o}')
    assert conn.size() == 0

    # explain
    q = conn.explain('select * {?s ?p ?o}')
    assert 'Projection(?s, ?p, ?o)' in q

    # query in transaction
    conn.begin()
    conn.add(Raw(data, TURTLE))

    q = conn.select('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 2

    conn.commit()

    # update in transaction
    conn.begin()
    q = conn.update('delete where {?s ?p ?o}')

    q = conn.select('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 0

    conn.commit()

def test_docs(conn, admin):
    content = 'Only the Knowledge Graph can unify all data types and every data velocity into a single, coherent, unified whole.'

    # docstore
    docs = conn.docs()
    assert docs.size() == 0

    # add
    docs.add('doc', Raw(content))
    assert docs.size() == 1

    # get
    doc = docs.get('doc')
    assert doc == content

    with docs.get('doc', stream=True, chunk_size=1) as doc:
        assert ''.join(doc) == content

    # delete
    docs.delete('doc')
    assert docs.size() == 0

    # clear
    docs.add('doc', Raw(content))
    assert docs.size() == 1
    docs.clear()
    assert docs.size() == 0

def test_icv(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    # add
    conn.begin()
    conn.add(Raw(data, TURTLE))
    conn.commit()

    icv = conn.icv()
    constraint = Raw('<urn:subj> <urn:pred> <urn:obj3> .', TURTLE)

    # add/remove/clear
    icv.add(constraint)
    icv.remove(constraint)
    icv.clear()

    # check/violations/convert
    assert icv.is_valid(constraint) == True
    assert len(icv.explain_violations(constraint)) == 2
    assert '<tag:stardog:api:context:all>' in icv.convert(constraint)

def test_vcs(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    vcs = conn.versioning()

    # commit
    conn.begin()
    conn.add(Raw(data, TURTLE))
    vcs.commit('a versioned commit')

    # select
    q = vcs.select('select distinct ?v {?v a vcs:Version}')
    assert len(q['results']['bindings']) > 0

    # paths
    q = vcs.paths('paths start ?x = vcs:user:admin end ?y = <http://www.w3.org/ns/prov#Person> via ?p')
    assert len(q['results']['bindings']) > 0

    # ask
    q = vcs.ask('ask {?v a vcs:Version}')
    assert q == True

    # construct
    q = vcs.graph('construct {?s ?p ?o} where {?s ?p ?o}')
    assert '<tag:stardog:api:versioning:Version>' in q

    # bindings
    q = vcs.select('select distinct ?v {?v a ?o}', bindings={'o': '<tag:stardog:api:versioning:Version>'})
    assert len(q['results']['bindings']) > 0

    # tags
    first_revision = q['results']['bindings'][0]['v']['value']
    second_revision = q['results']['bindings'][1]['v']['value']

    vcs.create_tag(first_revision, 'v.1')
    vcs.delete_tag('v.1')

    # revert
    vcs.revert(first_revision, second_revision, 'reverting')
