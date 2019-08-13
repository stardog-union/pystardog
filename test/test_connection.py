import pytest

import stardog.admin
import stardog.connection as connection
import stardog.content as content
import stardog.content_types as content_types


@pytest.fixture(scope="module")
def conn():
    with connection.Connection('newtest') as conn:
        yield conn


@pytest.fixture(scope="module")
def admin():
    with stardog.admin.Admin() as admin:

        for db in admin.databases():
            db.drop()

        admin.new_database('newtest', {
            'search.enabled': True
        })

        yield admin


def test_transactions(conn, admin):
    data = '<urn:subj> <urn:pred> <urn:obj> .'

    # add
    conn.begin()
    conn.clear()
    conn.add(content.Raw(data, content_types.TURTLE))
    conn.commit()

    assert conn.size() == 1

    # remove
    conn.begin()
    conn.remove(content.File('test/data/example.ttl.zip'))
    conn.commit()

    assert conn.size() == 0

    # rollback
    conn.begin()
    conn.add(content.Raw(data, content_types.TURTLE))
    conn.rollback()

    assert conn.size() == 0

    # export
    conn.begin()
    conn.add(
        content.URL('https://www.w3.org/2000/10/rdf-tests/'
                    'RDF-Model-Syntax_1.0/ms_4.1_1.rdf'))
    conn.commit()

    assert b'<http://description.org/schema/attributedTo>' in conn.export()

    with conn.export(stream=True, chunk_size=1) as stream:
        assert b'<http://description.org/schema/attributedTo>' in b''.join(
            stream)

    # clear
    conn.begin()
    conn.clear()
    conn.commit()

    assert conn.size() == 0


def test_queries(conn, admin):
    # add
    conn.begin()
    conn.clear()
    conn.add(content.File('test/data/starwars.ttl'))
    conn.commit()

    # select
    q = conn.select('select * {?s :name "Luke Skywalker"}')
    assert len(q['results']['bindings']) == 1

    # params
    q = conn.select('select * {?s a :Human}', offset=1, limit=10, timeout=1000)
    assert len(q['results']['bindings']) == 4

    # reasoning
    q = conn.select('select * {?s a :Character}', reasoning=True)
    assert len(q['results']['bindings']) == 7

    # no results with reasoning turned off
    q = conn.select('select * {?s a :Character}')
    assert len(q['results']['bindings']) == 0

    # the reasoning param on the query won't work in a transaction
    # that doesn't have reasoning enabled (the default)
    conn.begin()
    q = conn.select('select * {?s a :Character}', reasoning=True)
    assert len(q['results']['bindings']) == 0
    conn.rollback()

    # the query should return results if reasoning is on for the transaction
    conn.begin(reasoning=True)
    q = conn.select('select * {?s a :Character}', reasoning=True)
    assert len(q['results']['bindings']) == 7
    conn.rollback()

    # reasoning does not need to be specified in the query when it is
    # on in the transaction
    conn.begin(reasoning=True)
    q = conn.select('select * {?s a :Character}')
    assert len(q['results']['bindings']) == 7
    conn.rollback()

    # bindings
    q = conn.select(
        'select * {?s :name ?o}', bindings={'o': '"Luke Skywalker"'})
    assert len(q['results']['bindings']) == 1

    # paths
    q = conn.paths('paths start ?x = :luke end ?y = :leia via ?p')
    assert len(q['results']['bindings']) == 1

    # ask
    q = conn.ask('ask {:luke a :Droid}')
    assert not q

    # construct
    q = conn.graph('construct {:luke a ?o} where {:luke a ?o}')
    assert q.strip(
    ) == b'<http://api.stardog.com/luke> a <http://api.stardog.com/Human> .'

    # update
    q = conn.update('delete where {?s ?p ?o}')
    assert conn.size() == 0

    # explain
    q = conn.explain('select * {?s ?p ?o}')
    assert 'Projection(?s, ?p, ?o)' in q

    # query in transaction
    conn.begin()
    conn.add(content.File('test/data/starwars.ttl'))

    q = conn.select('select * {?s :name "Luke Skywalker"}')
    assert len(q['results']['bindings']) == 1

    conn.commit()

    # update in transaction
    conn.begin()
    q = conn.update('delete where {?s ?p ?o}')

    q = conn.select('select * {?s ?p ?o}')
    assert len(q['results']['bindings']) == 0

    conn.commit()


def test_docs(conn, admin):
    example = (b'Only the Knowledge Graph can unify all data types and '
               b'every data velocity into a single, coherent, unified whole.')

    # docstore
    docs = conn.docs()
    assert docs.size() == 0

    # add
    docs.add('doc', content.File('test/data/example.txt'))
    assert docs.size() == 1

    # get
    doc = docs.get('doc')
    assert doc == example

    with docs.get('doc', stream=True, chunk_size=1) as doc:
        assert b''.join(doc) == example

    # delete
    docs.delete('doc')
    assert docs.size() == 0

    # clear
    docs.add('doc', content.File('test/data/example.txt'))
    assert docs.size() == 1
    docs.clear()
    assert docs.size() == 0


def test_icv(conn, admin):

    conn.begin()
    conn.clear()
    conn.add(content.File('test/data/icv-data.ttl'))
    conn.commit()

    icv = conn.icv()
    constraints = content.File('test/data/icv-constraints.ttl')

    # check/violations/convert
    assert not icv.is_valid(constraints)
    assert len(icv.explain_violations(constraints)) == 14
    assert 'SELECT DISTINCT' in icv.convert(constraints)

    # add/remove/clear
    icv.add(constraints)
    icv.remove(constraints)
    icv.clear()


def test_graphql(conn, admin):

    db = admin.new_database('graphql', {},
                            content.File('test/data/starwars.ttl'))

    with connection.Connection(
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
        gql.add_schema(
            'characters', content=content.File('test/data/starwars.graphql'))

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

        gql.add_schema(
            'characters', content=content.File('test/data/starwars.graphql'))
        gql.clear_schemas()
        assert len(gql.schemas()) == 0

    db.drop()
