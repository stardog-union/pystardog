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

# RDF filename extensions and their content types
_RDF_EXTENSIONS = {
    'ttl': TURTLE,
    'rdf': RDF_XML,
    'rdfs': RDF_XML,
    'owl': RDF_XML,
    'xml': RDF_XML,
    'nt': NTRIPLES,
    'n3': N3,
    'nq': NQUADS,
    'nquads': NQUADS,
    'trig': TRIG,
    'trix': TRIX,
    'json': LD_JSON,
    'jsonld': LD_JSON
}

# Compression filename extensions and their content encodings
_COMPRESSION_EXTENSIONS = {'gz': 'gzip', 'zip': 'zip', 'bz2': 'bzip2'}


def guess_rdf_format(fname):
    """
    Guess RDF content type and encoding from filename

    Parameters
        fname (str)
            Filename

    Returns
        (tuple)
            (content_encoding, content_type)
    """
    extension = _get_extension(fname)

    # if compressed, needs content encoding
    content_encoding = _COMPRESSION_EXTENSIONS.get(extension)
    if content_encoding:
        # remove encoding extension
        extension = _get_extension(fname[:-len(extension) - 1])

    # get content type
    content_type = _RDF_EXTENSIONS.get(extension)

    return (content_encoding, content_type)


def _get_extension(fname):
    pos = fname.rfind('.')
    if pos >= 0:
        return fname[pos + 1:].lower()
