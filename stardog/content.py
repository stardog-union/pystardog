"""Content that can be loaded into Stardog.
"""
import contextlib
import os

import requests

from . import content_types as content_types


class Content(object):
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
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.name = name

    @contextlib.contextmanager
    def data(self):
        yield self.raw


class File(Content):
    """File-based content."""

    def __init__(self, fname, content_type=None, content_encoding=None, name=None):
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
        self.fname = fname
        (c_enc, c_type) = content_types.guess_rdf_format(fname)
        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_enc
        self.name = name if name else os.path.basename(fname)

    @contextlib.contextmanager
    def data(self):
        with open(self.fname, "rb") as f:
            yield f

class MappingRaw(Content):
    """User-defined Mapping.
    """

    def __init__(self,
                 content,
                 syntax=None,
                 name=None):
        """Initializes a Raw object.

        Args:
          content (str): Mapping in raw form
          syntax (str, optional): Whether it r2rml or sms type.
            It will be automatically detected from the filename, if possible otherwise it will default to system default
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
        self.syntax = syntax if syntax is not None else content_types.guess_mapping_format_from_content(content)
        self.name = name

    @contextlib.contextmanager
    def data(self):
        yield self.raw

class MappingFile(Content):
    """File-based content.
    """

    def __init__(self,
                 fname,
                 syntax=None,
                 name=None):
        """Initializes a File object.

        Args:
          fname (str): Filename
          syntax (str, optional): Whether it r2rml or sms type.
            It will be automatically detected from the filename, if possible otherwise it will default to system default

        Examples:
          >>> MappingFile('data.sms')
          >>> MappingFile('data.sms2')
          >>> MappingFile('data.rq')
          >>> MappingFile('data.r2rml')
        """
        self.fname = fname
        self.syntax = syntax if syntax is not None else content_types.guess_mapping_format(fname)
        self.name = name if name else os.path.basename(fname)

    @contextlib.contextmanager
    def data(self):
        with open(self.fname, 'rb') as f:
            yield f

class ImportFile(Content):
    """File-based content.
    """

    def __init__(self,
                 fname,
                 input_file_type=None,
                 separator=None,
                 name=None):
        """Initializes a File object.

        Args:
          fname (str): Filename
          syntax (str, optional): Whether it r2rml or sms type.
            It will be automatically detected from the filename, if possible otherwise it will default to system default

        Examples:
          >>> ImportFile('data.csv')
          >>> ImportFile('data.tsv')
          >>> ImportFile('data.txt','DELIMITED',"\t" )
          >>> MappingFile('data.json')
        """
        self.fname = fname
        if input_file_type is None or separator is None:
            (d_input_file_type,d_separator) = content_types.guess_import_format(fname)

            if input_file_type is None:
                input_file_type = d_input_file_type
                
            if separator is None:
                separator = d_separator

        self.name = name if name else os.path.basename(fname)

    @contextlib.contextmanager
    def data(self):
        with open(self.fname, 'rb') as f:
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
