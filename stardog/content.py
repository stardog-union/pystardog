import os
from contextlib import contextmanager

import requests

from stardog.content_types import guess_rdf_format


class Content(object):
    pass


class Raw(Content):
    """
    User-defined content
    """

    def __init__(self, content, content_type=None, content_encoding=None, name=None):
        """
        Parameters
            content (obj)
                Object representing the content (e.g., str, file)
            content_type (str)
                Content type (optional)
            content_encoding (str)
                Content encoding (optional)
            name (str)
                Object name (optional)

        Example
            >> Raw(':luke a :Human', 'text/turtle', name='data.ttl')
            >> Raw(open('data.ttl.zip', 'rb'), 'text/turtle', 'zip', 'data.ttl')
        """
        self.raw = content
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.name = name

    @contextmanager
    def data(self):
        yield self.raw


class File(Content):
    """
    File-based content
    """

    def __init__(self, fname, content_type=None, content_encoding=None, name=None):
        """
        Parameters
            fname (str)
                Filename
            content_type (str)
                Content type (optional). It will be automatically detected from the filename
            content_encoding (str)
                Content encoding (optional). It will be automatically detected from the filename
            name (str)
                Object name (optional). It will be automatically detected from the filename

        Example
            >> File('data.ttl')
            >> File('data.doc', 'application/msword')
        """
        self.fname = fname
        (c_encoding, c_type) = guess_rdf_format(fname)
        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_encoding
        self.name = name if name else os.path.basename(fname)

    @contextmanager
    def data(self):
        with open(self.fname, 'rb') as f:
            yield f


class URL(Content):
    """
    Url-based content
    """

    def __init__(self, url, content_type=None, content_encoding=None, name=None):
        """
        Parameters
            url (str)
                Url
            content_type (str)
                Content type (optional). It will be automatically detected from the url
            content_encoding (str)
                Content encoding (optional). It will be automatically detected from the filename
            name (str)
                Object name (optional). It will be automatically detected from the url

        Example
            >> Url('http://example.com/data.ttl')
            >> Url('http://example.com/data.doc', 'application/msword')
        """
        self.url = url
        (c_encoding, c_type) = guess_rdf_format(url)
        self.content_type = content_type if content_type else c_type
        self.content_encoding = content_encoding if content_encoding else c_encoding
        self.name = name if name else os.path.basename(url)

    @contextmanager
    def data(self):
        with requests.get(self.url, stream=True) as r:
            raw = r.raw
            # HTTPResponse, given by r.raw, is a proper file-like object, except missing a 'mode' property
            # This property is needed by requests lib to stream it back in a request, so we give it the expected mode, 'rb'
            raw.mode = 'rb'
            yield raw
