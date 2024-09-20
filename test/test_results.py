import pytest
import rdflib

from stardog import SelectQueryResult, content
from stardog.content_types import SPARQL_XML


@pytest.mark.dbname("pystardog-test-database")
@pytest.mark.conn_dbname("pystardog-test-database")
def test_SelectQueryResult(db, conn):
    conn.begin()
    conn.clear()
    conn.add(content.File("test/data/starwars.ttl"))
    conn.commit()

    results = SelectQueryResult(
        conn.select("select * {?s a :Human}", limit=10, timeout=1000)
    )
    assert results.variable_names == ["s"]
    assert len(results) == 5

    for binding_set in results:
        # automatic conversion to rdflib classes
        assert isinstance(binding_set.s, rdflib.URIRef)

        # can still retrieve the raw bindings
        raw_binding = binding_set.get_raw("s")
        assert isinstance(raw_binding, dict)
        assert raw_binding["type"] == "uri"
        assert raw_binding["value"]

        assert binding_set.variable_names == ["s"]


@pytest.mark.dbname("pystardog-test-database")
@pytest.mark.conn_dbname("pystardog-test-database")
def test_SelectQueryResult_raises_error_when_results_not_sparql_json(db, conn):
    conn.begin()
    conn.clear()
    conn.add(content.File("test/data/starwars.ttl"))
    conn.commit()

    with pytest.raises(ValueError):
        SelectQueryResult(
            conn.select(
                "select * {?s a :Human}",
                limit=10,
                timeout=1000,
                content_type=SPARQL_XML,
            )
        )
