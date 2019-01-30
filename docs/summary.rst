pystardog
=========

Python wrapper for communicating with the Stardog HTTP server.

What is it?
-----------

This framework wraps all the functionality of a client for the Stardog
DBMS, and provides access to a full set of functions such as executing
SPARQL queries, administrative tasks on Stardog, and the use of the
Reasoning API.

The implementation uses the HTTP protocol, since most of Stardog
functionality is available using this protocol. For more information,
go to the Stardog's `HTTP Programming
<http://www.stardog.com/docs/#_network_programming>`_ documentation.

Quick Example
-------------

.. code-block:: python

   conn = Connection('db',
                      endpoint='http://localhost:9999',
                      username='admin', password='admin')

    conn.select('select * {?s ?p ?o}',
                offset=100, limit=100, reasoning=True)
