import pytest

from stardog.content_types import TURTLE
from stardog.exceptions import StardogException
from stardog.http.admin import Admin
from stardog.http.client import Client
from stardog.http.connection import Connection


@pytest.fixture(scope="module")
def conn():
    with Connection('test', username='admin', password='admin') as conn:
        yield conn

@pytest.fixture(scope="module")
def admin():
    with Admin(username='admin', password='admin') as admin:

        for db in admin.databases():
            db.drop()
        
        admin.new_database('test', {'search.enabled': True, 'versioning.enabled': True})
        
        yield admin

def test_docs(conn, admin):
    content = 'Only the Knowledge Graph can unify all data types and every data velocity into a single, coherent, unified whole.'

    # docstore
    docs = conn.docs()
    assert docs.size() == 0

    # add
    docs.add('doc', content)
    assert docs.size() == 1
    assert conn.size() > 0

    # get
    doc = docs.get('doc')
    assert next(doc) == content

    # stream
    doc = docs.get('doc', stream=True, chunk_size=1)
    assert ''.join(next(doc)) == content

    # delete
    docs.delete('doc')
    assert docs.size() == 0

    # add from file
    with open('test/data/example.txt') as f:
        docs.add('example', f)
    
    assert docs.size() == 1
    
    doc = docs.get('example')
    assert next(doc) == content

    # clear
    docs.clear()
    assert docs.size() == 0

def test_transactions(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> .'

    # add
    t = conn.begin()
    conn.add(t, TURTLE, data)
    conn.commit(t)

    assert conn.size() == 1

    # remove
    t = conn.begin()
    conn.remove(t, TURTLE, data)
    conn.commit(t)

    assert conn.size() == 0

    # rollback
    t = conn.begin()
    conn.add(t, TURTLE, data)
    conn.rollback(t)

    assert conn.size() == 0

    # export
    t = conn.begin()
    conn.add(t, TURTLE, data)
    conn.commit(t)

    assert data in next(conn.export())

    # clear
    t = conn.begin()
    conn.clear(t)
    conn.commit(t)

    # add named graph
    t = conn.begin()
    conn.add(t, TURTLE, data, 'urn:graph')
    conn.commit(t)

    assert conn.size() == 1

    # remove from default graph
    t = conn.begin()
    conn.remove(t, TURTLE, data)
    conn.commit(t)

    assert conn.size() == 1

    # remove from named graph
    t = conn.begin()
    conn.remove(t, TURTLE, data, 'urn:graph')
    conn.commit(t)

    assert conn.size() == 0

def test_queries(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    # add
    t = conn.begin()
    conn.add(t, TURTLE, data)
    conn.commit(t)

    # query
    q = conn.query('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 2

    # params
    q = conn.query('select * {<urn:subj> ?p ?o}', offset=1, limit=1, timeout=1000, reasoning=True)
    assert len(q['results']['bindings']) == 1

    # bindings
    q = conn.query('select * {?s ?p ?o}', bindings={'o': '<urn:obj>'})
    assert len(q['results']['bindings']) == 1

    # construct
    q = conn.query('construct {?s ?p ?o} where {?s ?p ?o}', content_type=TURTLE)
    assert q.strip() == data

    # update
    q = conn.update('delete where {?s ?p ?o}')
    assert conn.size() == 0

    # explain
    q = conn.explain('select * {?s ?p ?o}')
    assert 'Projection(?s, ?p, ?o)' in q

    # query in transaction
    t = conn.begin()
    conn.add(t, TURTLE, data)

    q = conn.query('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 0

    q = conn.query('select * {?s ?p ?o}', transaction=t)
    assert len(q['results']['bindings']) == 2

    conn.commit(t)

    # update in transaction
    t = conn.begin()
    q = conn.update('delete where {?s ?p ?o}', transaction=t)

    q = conn.query('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 2

    q = conn.query('select * {?s ?p ?o}', transaction=t)
    assert len(q['results']['bindings']) == 0

    conn.commit(t)

def test_reasoning(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    # add
    t = conn.begin()
    conn.add(t, TURTLE, data)
    conn.commit(t)

    # consistency
    assert conn.is_consistent() == True

    # explain inference
    r = conn.explain_inference(TURTLE, '<urn:subj> <urn:pred> <urn:obj> .')
    assert len(r) == 1

    # explain inference in transaction
    t = conn.begin()
    conn.add(t, TURTLE, '<urn:subj> <urn:pred> <urn:obj3> .')

    # TODO server throws null pointer exception
    with pytest.raises(StardogException, match='There was an unexpected error on the server'):
        r = conn.explain_inference(TURTLE, '<urn:subj> <urn:pred> <urn:obj3> .', transaction=t)
        assert len(r) == 0
    
    # explain inconsistency in transaction
    # TODO server returns 404 Not Found!
    with pytest.raises(StardogException, match='Not Found!'):
        r = conn.explain_inconsistency(transaction=t)
        assert len(r) == 0

    conn.rollback(t)

    # explain inconsistency
    r = conn.explain_inconsistency()
    assert len(r) == 0

def test_icv(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    icv = conn.icv()

    # add/remove/clear
    icv.add(TURTLE, '<urn:subj> <urn:pred> <urn:obj3> .')
    icv.remove(TURTLE, '<urn:subj> <urn:pred> <urn:obj3> .')
    icv.clear()

    # check/violations/convert
    assert icv.is_valid(TURTLE, '<urn:subj> <urn:pred> <urn:obj3> .') == False
    assert len(icv.explain_violations(TURTLE, '<urn:subj> <urn:pred> <urn:obj3> .')) == 2
    assert '<tag:stardog:api:context:all>' in icv.convert(TURTLE, '<urn:subj> <urn:pred> <urn:obj3> .')

def test_vcs(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    vcs = conn.versioning()

    # commit
    t = conn.begin()
    conn.add(t, TURTLE, data)
    vcs.commit(t, 'a versioned commit')

    # query
    q = vcs.query('select distinct ?v {?v a vcs:Version}')
    assert len(q['results']['bindings']) > 0

    # tags
    first_revision = q['results']['bindings'][0]['v']['value']
    second_revision = q['results']['bindings'][1]['v']['value']

    vcs.create_tag(first_revision, 'v.1')
    vcs.delete_tag('v.1')

    # revert
    vcs.revert(first_revision, second_revision, 'reverting')


def test_graphql(conn, admin):

    with open('test/data/starwars.ttl') as f:
        db = admin.new_database('graphql', {}, {'name': 'starwars.ttl', 'content': f, 'content-type': TURTLE})
    
    with Connection('graphql', username='admin', password='admin') as c:
        gql = c.graphql()

        # query
        assert gql.query('{ Planet { system } }') == [{'system': 'Tatoo'}, {'system': 'Alderaan'}]

        # variables
        assert gql.query('query getHuman($id: Integer) { Human(id: $id) {name} }', variables={'id': 1000}) == [{'name': 'Luke Skywalker'}]

        # schemas
        with open('test/data/starwars.graphql') as f:
            gql.add_schema('characters', f)
        
        assert len(gql.schemas()) == 1
        assert 'type Human' in gql.schema('characters')

        assert gql.query('{Human(id: 1000) {name friends {name}}}', variables={'@schema': 'characters'}) == [{'friends': [{'name': 'Han Solo'}, {'name': 'Leia Organa'}, {'name': 'C-3PO'}, {'name': 'R2-D2'}], 'name': 'Luke Skywalker'}]

        gql.remove_schema('characters')
        assert len(gql.schemas()) == 0

        gql.clear_schemas()
        assert len(gql.schemas()) == 0

    db.drop()
