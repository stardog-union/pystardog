
# RDF
TURTLE = 'text/turtle'
RDF_XML = 'application/rdf+xml'
NTRIPLES = 'application/n-triples'
NQUADS = 'application/n-quads'
TRIG = 'application/trig'
N3 = 'text/n3'
TRIX = 'application/trix'
LD_JSON = 'application/ld+json'

# Query Results
SPARQL_XML = 'application/sparql-results+xml'
SPARQL_JSON = 'application/sparql-results+json'
BINARY_RDF = 'application/x-binary-rdf-results-table'
BOOLEAN = 'text/boolean'
CSV = 'text/csv'
TSV = 'text/tab-separated-values'

# Filename extension
_FILENAME_EXTENSIONS = {
    'ttl': TURTLE,
    'rdf': RDF_XML,
    'nt': NTRIPLES,
    'nq': NQUADS,
    'trig': TRIG,
    'n3': N3,
    'xml': TRIX,
    'jsonld': LD_JSON
}


def guess_rdf_format(fname):
    pos = fname.rfind('.')
    if pos >= 0:
        extension = fname[pos + 1:]
        return _FILENAME_EXTENSIONS.get(extension)
