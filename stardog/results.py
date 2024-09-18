"""
Module for working with query results.
"""

import copy
import reprlib
from collections import UserDict
from collections.abc import Sequence
from typing import Dict, Iterator, List, Literal, Optional, TypedDict, Union

from rdflib import BNode
from rdflib import Literal as RDFLiteral
from rdflib import URIRef


class RDFTerm(TypedDict, total=False):
    """An RDF term as its encoded in JSON in SPARQL JSON Query Results."""

    type: Literal["bnode", "uri", "literal"]
    """The type of RDF term encoded in this dict"""
    value: str
    """The value of the RDF term. For URIs, this is the URI itself;
    for literals, this is the value of the literal;
    for bnodes, this is the blank node identifier."""
    datatype: Optional[str]
    """The datatype of the literal. Not strictly required for literals."""
    lang: Optional[str]
    """The language tag of the literal. Not strictly required for literals."""


class SPARQLQueryResultsHead(TypedDict):
    """The ``head`` member of the SPARQL JSON Query results"""

    vars: List[str]
    """List of variable names in the result"""


class SPARQLQueryResults(TypedDict):
    """The ``results`` member of the SPARQL JSON Query results"""

    bindings: List[Dict[str, RDFTerm]]
    """
    Each binding is a dictionary of `variable_name: RDFTerm` representing a solution
    to the query.
    """


class SPARQLQueryResultsJSON(TypedDict):
    """
    Represents the `SPARQL Query results JSON format <https://www.w3.org/TR/2013/REC-sparql11-results-json-20130321>`_
    """

    head: SPARQLQueryResultsHead
    """Contains the variables projected in the select query results"""
    results: SPARQLQueryResults
    """Contains the solution(s) to the query."""


def rdf_term_to_rdflib(rdf_term: RDFTerm) -> Union[URIRef, RDFLiteral, BNode]:
    """
    Convert the JSON encoded RDF term from SPARQL JSON Query results to its `rdflib <https://rdflib.readthedocs.io/en/stable/intro_to_creating_rdf.html>`_ equivalent.

    Raises a ``ValueError`` if the type is unknown.
    """
    term_type = rdf_term["type"]
    value = rdf_term["value"]

    if term_type == "uri":
        return URIRef(value)

    if term_type == "literal":
        datatype = rdf_term.get("datatype")
        lang = rdf_term.get("lang")
        return RDFLiteral(value, datatype=datatype, lang=lang)

    if term_type == "bnode":
        return BNode(value)

    raise ValueError(f"Unsupported RDF term type: {term_type}")


class SelectQueryResult(Sequence):
    """
    A specialized sequence-like container for SPARQL SELECT query results.

    This class encapsulates the results returned from a SPARQL SELECT query
    in JSON format, providing a convenient interface for accessing and
    manipulating the query results. It implements the Sequence protocol,
    allowing for indexed access, iteration, and length operations.


    .. code-block:: python
        :caption: Iterate over result bindings

        >>> results = SelectQueryResult(conn.select('select * {?s ?p ?o} limit 10'))
        >>> for binding_set in results:
        ...     print(f"Type: {type(binding_set.s)} - {binding_set.s}")
        ...
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/%22Heroes%22_(David_Bowie_album)
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/artist
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/date
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/track
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/(Miss)understood
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/+_(Ed_Sheeran_album)
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/...And_Justice_for_All_(album)
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/...And_Then_There_Were_Three...
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/...Baby_One_More_Time_(album)
        Type: <class 'rdflib.term.URIRef'> - http://stardog.com/tutorial/...But_Seriously

    .. code-block:: python
        :caption: String representation of ``SelectQueryResult`` instance will show you an overview/preview of the result bindings.

        >>> results = SelectQueryResult(conn.select('select * {?s ?p ?o} limit 10'))
        >>> print(results)
        SelectQueryResult with 10 results
        Variables: s, p, o
        Preview of the first 3 results:
        ?s = http://stardog.com/tutorial/%22Heroes%22_(David_Bowie_album), ?p = http://www.w3.org/1999/02/22-rdf-syntax-ns#type, ?o = http://stardog.com/tutorial/Album
        ?s = http://stardog.com/tutorial/artist, ?p = http://www.w3.org/2000/01/rdf-schema#domain, ?o = http://stardog.com/tutorial/Album
        ?s = http://stardog.com/tutorial/date, ?p = http://www.w3.org/2000/01/rdf-schema#domain, ?o = http://stardog.com/tutorial/Album
        ...and 7 more rows.
    """

    def __init__(self, results: SPARQLQueryResultsJSON):
        self._validate(results)
        self._raw_results = results
        self._bindings = [BindingSet(x) for x in results["results"]["bindings"]]

    @staticmethod
    def _validate(results: SPARQLQueryResultsJSON) -> None:
        """Validate results adheres to SPARQL Query Results JSON format."""
        if not isinstance(results, dict):
            raise ValueError("Results must be a dictionary")
        if "head" not in results or "results" not in results:
            raise ValueError("Results must contain 'head' and 'results' keys")
        if (
            not isinstance(results["results"], dict)
            or "bindings" not in results["results"]
        ):
            raise ValueError("Results['results'] must contain a 'bindings' key")
        if not isinstance(results["results"]["bindings"], list):
            raise ValueError("Results['results']['bindings'] must be a list")

    @property
    def raw(self) -> SPARQLQueryResults:
        """Returns a deep copy of the underlying dict containing the raw SPARQL query results."""
        return copy.deepcopy(self._raw_results)

    @property
    def variable_names(self) -> List[str]:
        """The variables projected in the query."""
        return self._raw_results["head"]["vars"]

    @property
    def bindings(self) -> List["BindingSet"]:
        """The binding sets represents a solution to the query."""
        return self._bindings

    @property
    def is_empty(self) -> bool:
        """Whether there is one or more solution to the query or not."""
        return len(self._bindings) == 0

    def __len__(self) -> int:
        return len(self._bindings)

    def __iter__(self) -> Iterator["BindingSet"]:
        return iter(self._bindings)

    def __getitem__(self, index) -> "BindingSet":
        return self._bindings[index]

    def __repr__(self) -> str:
        bindings = reprlib.repr(self._bindings)
        return f"{self.__class__.__name__}({bindings})"

    def __str__(self) -> str:
        num_results = len(self._bindings)
        variables = ", ".join(self.variable_names)

        max_preview = 3
        preview_rows = "\n".join(
            str(binding) for binding in self._bindings[:max_preview]
        )

        result = f"{self.__class__.__name__} with {num_results} results\n"
        result += f"Variables: {variables}\n"

        if num_results > 0:
            result += f"Preview of the first {min(num_results, max_preview)} results:\n{preview_rows}"
            if num_results > max_preview:
                result += f"\n...and {num_results - max_preview} more rows."
        return result


class BindingSet(UserDict):
    """
    Represents a single result row (solution) from a SPARQL SELECT query.

    A ``BindingSet`` encapsulates a set of variable bindings, where each binding
    associates a variable name with an RDF term (URI, literal, or blank node).
    It provides a convenient interface for accessing these bindings, supporting
    both attribute-style and dictionary-style access.

    .. note::
        You typically shouldn't instantiate the ``BindingSet`` class directly. Instead,
        it is meant to be used as part of a ``SelectQueryResult`` object,
        which handles the conversion of SPARQL query results into ``BindingSet`` instances.
        To access the result bindings, use the ``SelectQueryResult`` class, which
        provides a more convenient interface for handling SPARQL SELECT query results.

    .. note::
        The typical accessors (e.g., ``binding_set.s`` or ``binding_set['s']``) return the converted ``rdflib`` equivalents,
        such as ``rdflib.term.URIRef``, ``rdflib.term.BNode``, or ``rdflib.term.Literal``. These converted objects can be used within the ``rdflib`` ecosystem for further manipulation or queries.

        If you need access to the raw RDF term exactly as it appears in the original SPARQL results JSON (before conversion),
        you can use the ``get_raw`` method. This allows you to retrieve the JSON representation of the RDF term, which includes
        details like type and value.

    .. code-block:: python
        :caption: Access bindings using attribute-style access

        >>> results = SelectQueryResult(conn.select('select * {?s ?p ?o} limit 10'))
        >>> for binding_set in results:
        ...     print(f"Subject: {binding_set.s}")
        ...
        Subject: http://stardog.com/tutorial/Album
        Subject: http://stardog.com/tutorial/artist

    .. code-block:: python
        :caption: Access bindings using dictionary-style access

        >>> results = SelectQueryResult(conn.select('select * {?s ?p ?o} limit 10'))
        >>> first_result = results[0]
        >>> print(f"Subject: {first_result['s']}")
        Subject: http://stardog.com/tutorial/Album
        >>> print(f"Predicate: {first_result['p']}")
        Predicate: http://www.w3.org/1999/02/22-rdf-syntax-ns#type
    """

    def __init__(self, bindings: Dict[str, RDFTerm]):
        self._raw = bindings
        converted_bindings = {
            key: rdf_term_to_rdflib(value) for key, value in bindings.items()
        }
        super().__init__(converted_bindings)

    @property
    def variable_names(self) -> List[str]:
        """Returns the variable names in this binding set."""
        return list(self.keys())

    @property
    def raw(self) -> Dict[str, RDFTerm]:
        """Returns a deep copy of the raw bindings from the actual SPARQL query JSON results."""
        return copy.deepcopy(self._raw)

    def get_raw(self, key: str, default_value=None):
        """
        Returns the raw, unconverted value for the variable binding,
        or a default if the key is not found.
        """
        return self._raw.get(key, default_value)

    def to_list(self) -> List[Union[URIRef, BNode, RDFLiteral]]:
        """Converts the binding set to a list of converted rdflib equivalents."""
        return list(self.values())

    def __getattr__(self, var: str) -> Union[URIRef, BNode, RDFLiteral]:
        if var in self:
            return self[var]
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{var}'"
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({reprlib.repr(self.data)})"

    def __str__(self) -> str:
        terms = [f"?{var} = {str(value)}" for var, value in self.items()]
        return f"{', '.join(terms)}"
