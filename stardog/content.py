"""Content that can be loaded into Stardog.
"""
import contextlib
import os
import uuid
from typing import Optional

import requests

from . import content_types as content_types


class Content:
    """Content base class."""

    pass


class Raw(Content):
    """User-defined content."""

    def __init__(
        self,
        content: object,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """Initializes a Raw object.

        :param content: Object representing the content (e.g., str, file)
        :param content_type: Content type
        :param content_encoding: Content encoding
        :param name: Object name

        Examples:
          >>> Raw(':luke a :Human', 'text/turtle', name='data.ttl')
          >>> Raw(':βüãäoñr̈ a :Employee .'.encode('utf-8'), 'text/turtle')

        """
        self.raw = content
        self.name = name

        (c_enc, c_type) = content_types.guess_rdf_format(name)
        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_enc

    @contextlib.contextmanager
    def data(self):
        yield self.raw


class File(Content):
    """File-based content."""

    def __init__(
        self,
        file: Optional[str] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        name: Optional[str] = None,
        fname: Optional[str] = None,
    ):
        """Initializes a File object.

        :param file: the filename/path of the file
        :param content_type: Content type.
            It will be automatically detected from the filename
        :param content_encoding: Content encoding.
            It will be automatically detected from the filename
        :param name: Name of the file object.
            It will be automatically detected from the filename
        :param fname: backward compatible parameter for ``file``

        Examples:
          >>> File('data.ttl')
          >>> File('data.doc', 'application/msword')
        """

        # file as a special meaning in IDE such as pycharm where it shows you a file picker. It helps you find the file
        # which is important for this type of call, but we need to be backward compatible in case they use fname=

        if fname:
            file = fname

        assert file, "Parameter file is required"

        self.fname = file
        (c_enc, c_type) = content_types.guess_rdf_format(file)
        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_enc
        self.name = name if name else os.path.basename(file)

    @contextlib.contextmanager
    def data(self):
        with open(self.fname, "rb") as f:
            yield f


class MappingRaw(Content):
    """User-defined Mapping."""

    def __init__(
        self, content: str, syntax: Optional[str] = None, name: Optional[str] = None
    ):
        """Initializes a MappingRaw object.

        :param content: the actual mapping content (e.g. ``'MAPPING\\n FROM SQL ...'``)
        :param syntax: The mapping syntax (``'STARDOG'``, ``'R2RML'``, or ``'SMS2'``)
                If not provided, it will try to detect it from ``name`` if provided, otherwise from the content itself
        :param name: name of object

        Examples:

            >>> mapping = '''
            MAPPING
            FROM SQL {
              SELECT *
              FROM `benchmark`.`person`
            }
            TO {
              ?subject rdf:type :person
            } WHERE {
              BIND(template("http://api.stardog.com/person/nr={nr}") AS ?subject)
            }
            '''
            >>> MappingRaw(mapping)

        """
        self.raw = content
        self.name = name

        c_syntax = None
        if name:
            c_syntax = content_types.guess_mapping_format(name)

        if c_syntax is None:
            c_syntax = content_types.guess_mapping_format_from_content(content)

        self.syntax = syntax if syntax else c_syntax

    @contextlib.contextmanager
    def data(self):
        yield self.raw


class MappingFile(Content):
    """File-based content."""

    def __init__(
        self, file: str, syntax: Optional[str] = None, name: Optional[str] = None
    ):
        """Initializes a File object.

        :param file: the filename/path of the file
        :param syntax: The mapping syntax (``'STARDOG'``, ``'R2RML'``, or ``'SMS2'``)
            If not provided, it will try to detect it from the ``file``'s extension.
        :param name: the name of the object. If not provided, will fall back to the basename of the ``file``.

        Examples:
          >>> MappingFile('data.sms')
          >>> MappingFile('data.sms2')
          >>> MappingFile('data.rq')
          >>> MappingFile('data.r2rml')
        """
        self.fname = file
        self.syntax = syntax if syntax else content_types.guess_mapping_format(file)
        self.name = name if name else os.path.basename(file)

    @contextlib.contextmanager
    def data(self):
        with open(self.fname, "rb") as f:
            yield f


class ImportRaw(Content):
    """User-defined content."""

    def __init__(
        self,
        content: object,
        input_type: Optional[str] = None,
        separator: Optional[str] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        name: Optional[str] = None,
        iri: Optional[str] = None,
    ):
        """Initializes a Raw object.

        :param content: Object representing the content (e.g., str, file)
        :param input_type: ``'DELIMITED'`` or ``'JSON'``
        :param separator: Required if ``input_type`` is ``'DELIMITED'``. Use ``','`` for a CSV. Use ``\\\\t`` for a TSV.
        :param content_type: Content type
        :param content_encoding: Content encoding
        :param name: Object name
        :param iri: IRI that uniquely identifies this content.
            It will default to "file://``name``" if omitted.

        .. note::
            if ``name`` is provided like a pseudo filename (i.e. ``'data.csv'``, ``'data.tsv'``, or ``'data.json'``), it will auto-detect most
            required parameters (``input_type``, ``separator``, ``content_type``, ``content_encoding``) - otherwise you must specify them.

        Examples:
          >>> ImportRaw('a,b,c',  name='data.csv')
          >>> ImportRaw('a\tb\tc', name='data.tsv')
          >>> ImportRaw({'foo':'bar'}, name='data.json')

        """
        self.raw = content
        self.name = name

        (c_enc, c_type, c_input_type, c_separator) = content_types.guess_import_format(
            name
        )

        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_enc
        self.input_type = input_type if input_type else c_input_type
        self.separator = separator if separator else c_separator
        self.iri = (
            iri if iri else f"file://{name}" if name else f"stream://{uuid.uuid4()}"
        )

    @contextlib.contextmanager
    def data(self):
        yield self.raw


class ImportFile(Content):
    """File-based content for Delimited and JSON file."""

    def __init__(
        self,
        file: str,
        input_type: Optional[str] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        separator: Optional[str] = None,
        name: Optional[str] = None,
        iri: Optional[str] = None,
    ):
        """Initializes a File object.

        :param file: filename/path of the file
        :param input_type: ``'DELIMITED'`` or ``'JSON'``
        :param content_type: Content type
        :param content_encoding: Content encoding
        :param separator: Required if ``input_type`` is ``'DELIMITED'``. Use ``','`` for a CSV. Use ``\\\\t`` for a TSV.
        :param name: Object name.
            It will be automatically detected from the ``file`` if omitted.
        :param iri: IRI that uniquely identifies this file.
            It will default to "file://``name``" if omitted.


        .. note::
            If ``file`` has a recognized extension (i.e. ``'data.csv'``, ``'data.tsv'``, or ``'data.json'``), it will auto-detect most
            required parameters (``input_type``, ``separator``, ``content_type``, ``content_encoding``) - otherwise you must specify them.

        Examples:
          >>> ImportFile('data.csv')
          >>> ImportFile('data.tsv')
          >>> ImportFile('data.txt','DELIMITED',"\\\\t" )
          >>> ImportFile('data.json')
        """

        self.fname = file

        (c_enc, c_type, c_input_type, c_separator) = content_types.guess_import_format(
            file
        )

        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_enc
        self.input_type = input_type if input_type else c_input_type
        self.separator = separator if separator else c_separator

        self.name = name if name else os.path.basename(file)
        self.iri = iri if iri else f"file://{self.name}"

    @contextlib.contextmanager
    def data(self):
        with open(self.fname, "rb") as f:
            yield f


class URL(Content):
    """Url-based content."""

    def __init__(
        self,
        url: str,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        name: Optional[str] = None,
    ):
        """Initializes a URL object.

        :param url: URL to the content
        :param content_type: Content type.
            It will be automatically detected from the ``url`` if not provided.
        :param content_encoding: Content encoding.
            It will be automatically detected from the ``url`` if not provided.
        :param name: Object name.
            It will be automatically detected from the ``url`` if not provided.

        Examples:
          >>> URL('http://example.com/data.ttl')
          >>> URL('http://example.com/data.doc', 'application/msword')
        """
        self.url = url
        (c_enc, c_type) = content_types.guess_rdf_format(url)
        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_enc
        self.name = name if name else os.path.basename(url)

    @contextlib.contextmanager
    def data(self):
        with requests.get(self.url, stream=True) as r:
            yield r.content
