import re
from typing import Optional

# An absolute IRI: a scheme (no delimiters, no '/'), then ':', then anything
# except the RFC 3987 delimiters and control characters (U+0000-U+0020).
_IRI = re.compile(r"""[^\x00-\x20<>"{}|^`\\/:]+:[^\x00-\x20<>"{}|^`\\]*""")


def validate_iri(iri: Optional[str]) -> None:
    """Raises ``ValueError`` unless ``iri`` is a valid absolute IRI.

    ``None`` is accepted: it means no graph URI was given, i.e. the default graph.
    """
    if iri is None:
        return
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
