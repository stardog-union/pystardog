import re

# RDF
TURTLE = "text/turtle"
RDF_XML = "application/rdf+xml"
NTRIPLES = "application/n-triples"
NQUADS = "application/n-quads"
TRIG = "application/trig"
N3 = "text/n3"
TRIX = "application/trix"
LD_JSON = "application/ld+json"

# Query Results
SPARQL_XML = "application/sparql-results+xml"
SPARQL_JSON = "application/sparql-results+json"
BINARY_RDF = "application/x-binary-rdf-results-table"
BOOLEAN = "text/boolean"
CSV = "text/csv"
TSV = "text/tab-separated-values"

# RDF filename extensions and their content types
_RDF_EXTENSIONS = {
    "ttl": TURTLE,
    "rdf": RDF_XML,
    "rdfs": RDF_XML,
    "owl": RDF_XML,
    "xml": RDF_XML,
    "nt": NTRIPLES,
    "n3": N3,
    "nq": NQUADS,
    "nquads": NQUADS,
    "trig": TRIG,
    "trix": TRIX,
    "json": LD_JSON,
    "jsonld": LD_JSON,
}
# Mapping filename extensions and their mapping syntax
_MAPPING_EXTENSIONS = {"rq": "SMS2", "sms": "SMS2", "sms2": "SMS2", "r2rml": "R2RML"}

# Import filename extension and their type, and separator
_IMPORT_EXTENSIONS = {
    "csv": (CSV, "DELIMITED", ","),
    "tsv": (TSV, "DELIMITED", "\\t"),
    "json": ("application/json", "JSON", None),
}

# Compression filename extensions and their content encodings
_COMPRESSION_EXTENSIONS = {"gz": "gzip", "zip": "zip", "bz2": "bzip2"}


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

    if fname is None:
        return None, None

    extension = _get_extension(fname)

    # if compressed, needs content encoding
    content_encoding = _COMPRESSION_EXTENSIONS.get(extension)
    if content_encoding:
        # remove encoding extension
        extension = _get_extension(fname[: -len(extension) - 1])

    # get content type
    content_type = _RDF_EXTENSIONS.get(extension)

    return content_encoding, content_type


def guess_mapping_format(fname):
    """
    Guess mapping syntax from filename

    Parameters
        fname (str)
            Filename

    Returns
            syntax
    """

    if fname is None:
        return None

    extension = _get_extension(fname)

    syntax = _MAPPING_EXTENSIONS.get(extension)

    return syntax


def guess_import_format(fname):
    """
    Guess import syntax from filename

    Parameters
        fname (str)
            Filename

    Returns
            (input_file_type,separator)
    """

    if fname is None:
        return None, None, None, None

    extension = _get_extension(fname)

    content_encoding = _COMPRESSION_EXTENSIONS.get(extension)
    if content_encoding:
        # remove encoding extension
        extension = _get_extension(fname[: -len(extension) - 1])

    # get content type
    info = _IMPORT_EXTENSIONS.get(extension)

    content_type = info[0] if info else None
    input_type = info[1] if info else None
    separator = info[2] if info else None

    return content_encoding, content_type, input_type, separator


def guess_mapping_format_from_content(content):
    """
    Guess mapping syntax from content

    Parameters
        fname (str)
            Filename

    Returns
            syntax
    """
    regex = re.compile("MAPPING.*?FROM", re.DOTALL | re.IGNORECASE)
    syntax = "SMS2" if regex.search(content) else None

    return syntax


def _get_extension(fname):
    pos = fname.rfind(".")
    if pos >= 0:
        return fname[pos + 1 :].lower()
    raise Exception("File has no extension")
