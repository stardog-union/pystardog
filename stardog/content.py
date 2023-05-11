"""Content that can be loaded into Stardog.
"""
import contextlib
import os

import requests

from . import content_types as content_types


class Content:
    """Content base class."""

    pass


class Raw(Content):
    """User-defined content."""

    def __init__(self, content, content_type=None, content_encoding=None, name=None):
        """Initializes a Raw object.

        Args:
          content (obj): Object representing the content (e.g., str, file)
          content_type (str, optional): Content type
          content_encoding (str, optional): Content encoding
          name (str, optional): Object name

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
        self, file=None, content_type=None, content_encoding=None, name=None, fname=None
    ):
        """Initializes a File object.

        Args:
          fname (str): Filename
          content_type (str, optional): Content type.
            It will be automatically detected from the filename
          content_encoding (str, optional): Content encoding.
            It will be automatically detected from the filename
          name (str, optional): Object name.
            It will be automatically detected from the filename

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

    def __init__(self, content, syntax=None, name=None):
        """Initializes a Raw object.

                Args:
                  content (str): Mapping in raw form
                  syntax (str, optional): Whether it r2rml or sms type.
                    If not provided, it will try to detect it from name if provided, otherwise from the content itselft
                  name (str, optional): Object name

                Examples:
                  >>> MappingRaw('''MAPPING
        FROM SQL {
          SELECT *
          FROM `benchmark`.`person`
        }
        TO {
          ?subject rdf:type :person
        } WHERE {
          BIND(template("http://api.stardog.com/person/nr={nr}") AS ?subject)
        }''')
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

    def __init__(self, file: str, syntax=None, name=None):
        """Initializes a File object.

        Args:
          file (str): Filename
          syntax (str, optional): Whether it r2rml or sms type.
            It will be automatically detected from the filename, if possible otherwise it will default to system default

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
        content,
        input_type=None,
        separator=None,
        content_type=None,
        content_encoding=None,
        name=None,
    ):
        """Initializes a Raw object.

        Args:
          content (obj): Object representing the content (e.g., str, file)
          input_type (str): DELIMITED or JSON
          separator (str): Required if it's DELIMITED CONTENT
          content_type (str, optional): Content type
          content_encoding (str, optional): Content encoding
          name (str, optional): Object name

          if name is provided like a pseudo filename, ie data.csv, data.tsv, or data.json, it will auto-detect most
          required parameter, otherwise you must specify them.

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

    @contextlib.contextmanager
    def data(self):
        yield self.raw


class ImportFile(Content):
    """File-based content for Delimited and JSON file."""

    def __init__(
        self,
        file,
        input_type=None,
        content_type=None,
        content_encoding=None,
        separator=None,
        name=None,
    ):
        """Initializes a File object.

        Args:
          file (str): Filename
          input_type (str): DELIMITED or JSON
          content_type (str, optional): Content type
          content_encoding (str, optional): Content encoding
          separator (str): Required if it's DELIMITED CONTENT
          name (str, optional): Object name
            It will be automatically detected from the filename, if possible otherwise it will default to system default

        Examples:
          >>> ImportFile('data.csv')
          >>> ImportFile('data.tsv')
          >>> ImportFile('data.txt','DELIMITED',"\t" )
          >>> MappingFile('data.json')
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

    @contextlib.contextmanager
    def data(self):
        with open(self.fname, "rb") as f:
            yield f


class URL(Content):
    """Url-based content."""

    def __init__(self, url, content_type=None, content_encoding=None, name=None):
        """Initializes a URL object.

        Args:
          url (str): Url
          content_type (str, optional): Content type.
              It will be automatically detected from the url
          content_encoding (str, optional): Content encoding.
              It will be automatically detected from the filename
          name (str, optional): Object name.
              It will be automatically detected from the url

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
