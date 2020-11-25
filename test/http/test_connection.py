import pytest

import stardog.content_types as content_types
import stardog.exceptions as exceptions
import stardog.admin as stardog_admin
import stardog.http.connection as http_connection
import stardog.content as content


@pytest.fixture(scope="module")
def conn():
    with http_connection.Connection('test') as conn:
        yield conn


@pytest.fixture(scope="module")
def admin():
    with stardog_admin.Admin() as admin:

        for db in admin.databases():
            db.drop()

        admin.new_database('test', {
            'search.enabled': True
        })

        yield admin


def test_transactions(conn, admin):
    data = b'<urn:subj> <urn:pred> <urn:obj> .'

    # add
    t = conn.begin()
    conn.add(t, data, content_types.TURTLE)
    conn.commit(t)

    assert conn.size(exact=True) == 1

    # remove
    t = conn.begin()
    conn.remove(t, data, content_types.TURTLE)
    conn.commit(t)

    assert conn.size(exact=True) == 0

    # rollback
    t = conn.begin()
    conn.add(t, data, content_types.TURTLE)
    conn.rollback(t)

    assert conn.size(exact=True) == 0

    # export
    t = conn.begin()
    conn.add(t, data, content_types.TURTLE)
    conn.commit(t)

    assert data in next(conn.export())

    # clear
    t = conn.begin()
    conn.clear(t)
    conn.commit(t)

    # add named graph
    t = conn.begin()
    conn.add(t, data, content_types.TURTLE, graph_uri='urn:graph')
    conn.commit(t)

    assert conn.size(exact=True) == 1

    # remove from default graph
    t = conn.begin()
    conn.remove(t, data, content_types.TURTLE)
    conn.commit(t)

    assert conn.size(exact=True) == 1

    # remove from named graph
    t = conn.begin()
    conn.remove(t, data, content_types.TURTLE, graph_uri='urn:graph')
    conn.commit(t)

    assert conn.size(exact=True) == 0


def test_queries(conn, admin):
    data = b'<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    # add
    t = conn.begin()
    conn.add(t, data, content_types.TURTLE)
    conn.commit(t)

    # query
    q = conn.query('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 2

    # params
    q = conn.query(
        'select * {<urn:subj> ?p ?o}',
        offset=1,
        limit=1,
        timeout=1000,
        reasoning=True)
    assert len(q['results']['bindings']) == 1

    # bindings
    q = conn.query('select * {?s ?p ?o}', bindings={'o': '<urn:obj>'})
    assert len(q['results']['bindings']) == 1

    # construct
    q = conn.query(
        'construct {?s ?p ?o} where {?s ?p ?o}',
        content_type=content_types.TURTLE)
    assert q.strip() == data

    # update
    q = conn.update('delete where {?s ?p ?o}')
    assert conn.size(exact=True) == 0

    # explain
    q = conn.explain('select * {?s ?p ?o}')
    assert 'Projection(?s, ?p, ?o)' in q

    # query in transaction
    t = conn.begin()
    conn.add(t, data, content_types.TURTLE)

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
    data = b'<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .'

    # add
    t = conn.begin()
    conn.add(t, data, content_types.TURTLE)
    conn.commit(t)

    # consistency
    assert conn.is_consistent()

    # explain inference
    r = conn.explain_inference('<urn:subj> <urn:pred> <urn:obj> .',
                               content_types.TURTLE)
    assert ('status', 'ASSERTED') in r[0].items()

    # explain inference in transaction
    t = conn.begin()
    conn.add(t, '<urn:subj> <urn:pred> <urn:obj3> .', content_types.TURTLE)

    with pytest.raises(
            exceptions.StardogException):
        r = conn.explain_inference(
            '<urn:subj> <urn:pred> <urn:obj3> .',
            content_types.TURTLE,
            transaction=t)
        assert len(r) == 0

    # explain inconsistency in transaction
    with pytest.raises(
            exceptions.StardogException):
        r = conn.explain_inconsistency(transaction=t)
        assert len(r) == 0

    conn.rollback(t)

    # explain inconsistency
    r = conn.explain_inconsistency()
    assert len(r) == 0


def test_graphql(conn, admin):

    db = admin.new_database('graphql', {},
                            content.File('test/data/starwars.ttl'))

    with http_connection.Connection(
            'graphql', username='admin', password='admin') as c:
        gql = c.graphql()

        # query
        assert gql.query('{ Planet { system } }') == [{
            'system': 'Tatoo'
        }, {
            'system': 'Alderaan'
        }]

        # variables
        assert gql.query(
            'query getHuman($id: Integer) { Human(id: $id) {name} }',
            variables={'id': 1000}) == [{
                'name': 'Luke Skywalker'
            }]

        # schemas
        with open('test/data/starwars.graphql', 'rb') as f:
            gql.add_schema('characters', f)

        assert len(gql.schemas()) == 1
        assert 'type Human' in gql.schema('characters')

        assert gql.query(
            '{Human(id: 1000) {name friends {name}}}',
            variables={'@schema': 'characters'}) == [{
                'friends': [{
                    'name': 'Han Solo'
                }, {
                    'name': 'Leia Organa'
                }, {
                    'name': 'C-3PO'
                }, {
                    'name': 'R2-D2'
                }],
                'name':
                'Luke Skywalker'
            }]

        gql.remove_schema('characters')
        assert len(gql.schemas()) == 0

        gql.clear_schemas()
        assert len(gql.schemas()) == 0

    db.drop()
