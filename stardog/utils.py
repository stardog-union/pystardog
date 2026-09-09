import re
from typing import Optional

# An absolute IRI: an RFC 3986 scheme -- ALPHA *( ALPHA / DIGIT / "+" / "-" /
# "." ) -- then ':', then a non-empty remainder free of the RFC 3987
# delimiters and control characters (U+0000-U+0020). The scheme was
# previously "anything without a delimiter", which accepted '?:x', '1abc:x',
# 'a?b:c' and 'graph#a:b'; the remainder was optional, which accepted 'urn:'.
_IRI = re.compile(r"""[A-Za-z][A-Za-z0-9+.\-]*:[^\x00-\x20<>"{}|^`\\]+""")


def validate_iri(iri: Optional[str]) -> None:
    """Raises ``ValueError`` unless ``iri`` is a valid absolute IRI.

    ``None`` is accepted: it means no graph URI was given, i.e. the default graph.
    """
    if iri is None:
        return
    if not isinstance(iri, str):
        # A sequence used to reach the server untouched. These parameters are
        # typed Optional[str]; where this API accepts several graphs it says
        # so, as using_graph_uri does. Rejecting here keeps the failure a
        # ValueError like every other bad graph URI, rather than a TypeError
        # out of re.fullmatch.
        raise ValueError(
            f"graph URI must be a string, got {type(iri).__name__}: {iri!r}"
        )
    if not _IRI.fullmatch(iri):
        raise ValueError(f"not a valid IRI: {iri!r}")


def strtobool(s):
    truthy_values = ["y", "yes", "t", "true", "True", "on", 1]
    falsy_values = ["n", "no", "f", "false", "False", "off", 0]
    if s in truthy_values:
        return True
    if s in falsy_values:
        return False
    raise ValueError
