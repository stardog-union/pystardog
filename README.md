# pystardog

Python wrapper for communicating with the Stardog HTTP server.

## What is it?

This framework wraps all the functionality of a client for the Stardog
Knowledge Graph, and provides access to a full set of functions such
as executing SPARQL queries, administrative tasks on Stardog, and the
use of the Reasoning API.

The implementation uses the HTTP protocol, since most of Stardog
functionality is available using this protocol. For more information,
go to the Stardog's [HTTP
Programming](http://www.stardog.com/docs/#_network_programming)
documentation.

## Installation

pystardog is on PyPI so all you need is: `pip install pystardog`

## Documentation

Documentation is readable at [Read the
Docs](http://pystardog.readthedocs.io) or can be built using Sphinx:

    pip install -r requirements.txt
    cd docs
    make html

## Tests

Run the tests with: `python setup.py test`

## Quick Example

```python
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
```

## Interactive Tutorial

There is a Jupyter notebook and instructions in the `notebooks`
directory of this repository.
