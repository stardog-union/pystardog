import re
from typing import Optional

# An absolute IRI: an RFC 3986 scheme -- ALPHA *( ALPHA / DIGIT / "+" / "-" /
# "." ) -- then ':', then a non-empty remainder free of the RFC 3987
# delimiters and control characters (U+0000-U+0020). The scheme was
# previously "anything without a delimiter", which accepted '?:x', '1abc:x',
# 'a?b:c' and 'graph#a:b'; the remainder was optional, which accepted 'urn:'.
_IRI = re.compile(r"""[A-Za-z][A-Za-z0-9+.\-]*:[^\x00-\x20<>"{}|^`\\]+""")


# Only the endpoints that read a graph parameter through
# ProtocolUtils.parameterAsGraph map "default", case-insensitively, to the
# default graph rather than parsing it as an IRI: see
# HTTPProtocol.PARAM_VALUE_DEFAULT. Elsewhere -- insert_graph_uri, a virtual
# graph's named_graph -- it is just a relative IRI that names a graph called
# "default", so callers opt in with allow_default.
_DEFAULT_GRAPH = "default"


def validate_iri(
    iri: Optional[str], param: str = "graph URI", allow_default: bool = False
) -> None:
    """Raises ``ValueError`` unless ``iri`` is a valid absolute IRI.

    ``None`` is accepted: it means the argument was not supplied.

    :param param: name of the argument being checked, used in the error message.
    :param allow_default: also accept the literal ``"default"``, in any case,
        for the parameters the server reads as a reference to the default
        graph rather than as an IRI.
    """
    if iri is None:
        return
    if not isinstance(iri, str):
        # A sequence used to reach the server untouched. Rejecting here keeps
        # the failure a ValueError like every other bad graph URI, rather than
        # a TypeError out of re.fullmatch.
        raise ValueError(f"{param} must be a string, got {type(iri).__name__}: {iri!r}")
    if allow_default and iri.lower() == _DEFAULT_GRAPH:
        return
    if not _IRI.fullmatch(iri):
        raise ValueError(f"{param} is not a valid IRI: {iri!r}")


def strtobool(s):
    truthy_values = ["y", "yes", "t", "true", "True", "on", 1]
    falsy_values = ["n", "no", "f", "false", "False", "off", 0]
    if s in truthy_values:
        return True
    if s in falsy_values:
        return False
    raise ValueError
