# pystardog

Python wrapper for communicating with the Stardog HTTP server.

## What is it?

This framework wraps all the functionality of a client for the Stardog
DBMS, and provides access to a full set of functions such as executing
SPARQL queries, administrative tasks on Stardog, and the use of the
Reasoning API.

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

    pip install -r docs/requirements.txt
    cd docs
    make html

## Quick Example

```python
import stardog

admin = stardog.Admin(endpoint='http://localhost:5820',
                      username='admin', password='admin')

db = admin.new_database('db')

conn = stardog.Connection('db',
                          endpoint='http://localhost:5820',
                          username='admin', password='admin')

conn.begin()
conn.add(stardog.content.File('./test/data/example.ttl'))
conn.commit()

conn.select('select * { ?a ?p ?o }')

db.drop()
```
