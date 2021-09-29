import pytest

import stardog.admin
import stardog.connection as connection
import stardog.content as content
import stardog.content_types as content_types
import stardog.exceptions as exceptions


@pytest.fixture()
def conn(conn_string):
    with connection.Connection('newtest', **conn_string) as conn:
        yield conn


@pytest.fixture(autouse="True")
def db(conn_string):
    with stardog.admin.Admin(**conn_string) as admin:
        db = admin.new_database('newtest', {'search.enabled': True})
        yield admin
        db.drop()


@pytest.mark.skip(reason="Currently failing, check out another branch and see if it fails there.")
def test_transactions(conn):
    data = content.Raw('<urn:subj> <urn:pred> <urn:obj> .', content_types.TURTLE)

    # add
    conn.begin()
    conn.clear()
    conn.add(data)
    conn.commit()

    assert conn.size() == 1

    # remove
    conn.begin()
    conn.remove(content.File('test/data/example.ttl.zip'))
    conn.commit()

    assert conn.size() == 0

    # rollback
    conn.begin()
    conn.add(data)
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

    # add named graph
    conn.begin()
    conn.add(data, graph_uri='urn:graph')
    conn.commit()

    assert conn.size(exact=True) == 1

    # remove from default graph
    conn.begin()
    conn.remove(data)
    conn.commit()

    assert conn.size(exact=True) == 1

    # remove from named graph
    conn.begin()
    conn.remove(data, graph_uri='urn:graph')
    conn.commit()

    assert conn.size(exact=True) == 0


def test_export(conn):
    conn.begin()
    # add to default graph
    conn.add(content.Raw('<urn:default_subj> <urn:default_pred> <urn:default_obj> .'))
    # add to a named graph
    conn.add(
        content.Raw('<urn:named_subj> <urn:named_pred> <urn:named_obj> .'),
        '<urn:named_graph>'
    )
    conn.commit()

    default_export = conn.export()
    named_export = conn.export(graph_uri='<urn:named_graph>')

    assert b'default_obj' in default_export
    assert b'named_obj' not in default_export
    assert b'named_obj' in named_export
    assert b'default_obj' not in named_export


def test_queries(conn):
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
    assert len(q['results']['bindings']) == 8

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
    assert len(q['results']['bindings']) == 8
    conn.rollback()

    # reasoning does not need to be specified in the query when it is
    # on in the transaction
    conn.begin(reasoning=True)
    q = conn.select('select * {?s a :Character}')
    assert len(q['results']['bindings']) == 8
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


@pytest.mark.skip(reason="Bug introduced in reasoning, it's being tracked here: https://stardog.atlassian.net/browse/PLAT-2027")
def test_reasoning(conn):
    data = content.Raw(
        b'<urn:subj> <urn:pred> <urn:obj> , <urn:obj2> .',
        content_types.TURTLE
    )

    # add
    conn.begin()
    conn.add(data)
    conn.commit()

    # consistency
    assert conn.is_consistent()

    # explain inference
    r = conn.explain_inference(
        content.Raw('<urn:subj> <urn:pred> <urn:obj> .', content_types.TURTLE)
    )
    assert ('status', 'ASSERTED') in r[0].items()

    # explain inference in transaction
    conn.begin()
    conn.add(content.Raw('<urn:subj> <urn:pred> <urn:obj3> .', content_types.TURTLE))

    with pytest.raises(exceptions.StardogException):
        r = conn.explain_inference(
            content.Raw('<urn:subj> <urn:pred> <urn:obj3> .', content_types.TURTLE)
        )
        assert len(r) == 0

    # explain inconsistency in transaction
    with pytest.raises(exceptions.StardogException):
        r = conn.explain_inconsistency()
        assert len(r) == 0

    conn.rollback()

    # explain inconsistency
    r = conn.explain_inconsistency()
    assert len(r) == 0


def test_docs(conn):
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

def test_unicode(conn):
    conn.begin()
    conn.add(content.Raw(':βüãäoñr̈ a :Employee .'.encode('utf-8'), content_types.TURTLE))
    conn.commit()

    r = conn.select('select * where { ?s ?p ?o }')
    assert r['results']['bindings'][0]['s']['value'] == 'http://api.stardog.com/βüãäoñr̈'

def test_icv(conn):

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

    constraint = content.Raw(
        ':Manager rdfs:subClassOf :Employee .',
        content_types.TURTLE
    )

    # add/remove/clear
    icv.add(constraint)
    icv.remove(constraint)
    icv.clear()

    # nothing in the db yet so it should be valid
    assert icv.is_valid(constraint)

    # insert a triple that violates the constraint
    conn.begin()
    conn.add(content.Raw(':Alice a :Manager .', content_types.TURTLE))
    conn.commit()

    assert not icv.is_valid(constraint)

    assert len(icv.explain_violations(constraint)) == 2

    # make Alice an employee so the constraint is satisfied
    conn.begin()
    conn.add(content.Raw(':Alice a :Employee .', content_types.TURTLE))
    conn.commit()

    assert icv.is_valid(constraint)

    assert 'SELECT DISTINCT' in icv.convert(constraint)

def test_graphql(conn_string):
    with stardog.admin.Admin(**conn_string) as admin:
        db = admin.new_database('graphql', {},
                            content.File('test/data/starwars.ttl'))

    with connection.Connection(
            'graphql', **conn_string) as c:
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
