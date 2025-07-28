Overview
========

.. image:: https://badge.fury.io/py/pystardog.svg
   :target: https://badge.fury.io/py/pystardog
   :alt: PyPI version

pystardog is a Python wrapper for communicating with the Stardog HTTP server.

**Requirements**: Python 3.8+

What is it?
-----------

This library wraps all the functionality of a client for the Stardog
Knowledge Graph, and provides access to a full set of functions such
as executing SPARQL queries and many administrative tasks.

The implementation uses the HTTP protocol, since most of Stardog
functionality is available using this protocol. For more information,
see `HTTP Programming <https://docs.stardog.com/developing/http-api>`_
in Stardog's documentation.

Installation
------------

pystardog is on `PyPI <https://pypi.org/project/pystardog/>`_. To install:

.. code-block:: shell

   pip install pystardog

Quick Example
-------------

.. code-block:: python

   import stardog

   conn_details = {
     'endpoint': 'http://localhost:5820',
     'username': 'admin',
     'password': 'admin'
   }

   with stardog.Admin(**conn_details) as admin:
     db = admin.new_database('db')

     with stardog.Connection('db', **conn_details) as conn:
       conn.begin()
       conn.add(stardog.content.File('./test/data/example.ttl'))
       conn.commit()
       results = conn.select('select * { ?a ?p ?o }')

     db.drop()

