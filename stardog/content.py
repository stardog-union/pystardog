import os
from contextlib import contextmanager

import requests

from stardog.content_types import guess_rdf_format


class Content(object):
    pass


class Raw(Content):

    def __init__(self, content, content_type=None, name=None):
        self.raw = content
        self.content_type = content_type
        self.name = name

    @contextmanager
    def data(self):
        yield self.raw


class File(Content):

    def __init__(self, fname, content_type=None, name=None):
        self.fname = fname
        self.content_type = content_type if content_type else guess_rdf_format(fname)
        self.name = name if name else os.path.basename(fname)

    @contextmanager
    def data(self):
        with open(self.fname, 'rb') as f:
            yield f


class URL(Content):

    def __init__(self, url, content_type=None, name=None):
        self.url = url
        self.content_type = content_type if content_type else guess_rdf_format(url)
        self.name = name if name else os.path.basename(url)

    @contextmanager
    def data(self):
        with requests.get(self.url, stream=True) as r:
            raw = r.raw
            # HTTPResponse, given by r.raw, is a proper file-like object, except missing a 'mode' property
            # This property is needed by requests lib to stream it back in a request, so we give it the expected mode, 'rb'
            raw.mode = 'rb'
            yield raw
