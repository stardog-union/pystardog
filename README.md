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

    cd docs
    pip install -r requirements.txt
    make html

## Tests

To run the tests locally, a valid Stardog license is required and placed in the `dockerfiles/stardog-license-key.bin`. 
Docker and docker-compose are also required.

1) Bring a stardog instance using docker-compose. For testing about 90% of the  pystardog features, just a single node is sufficient,
although we also provide a cluster set up for further testing. 
```shell script
# Bring a single node instance plus a bunch of Virtual Graphs for testing (Recommended).
docker-compose -f docker-compose.single-node.yml up -d

# A cluster set up is also provided, if cluster only features are to be implemented and tested.
docker-compose -f docker-compose.cluster.yml up -d
```

Run the test suite. Create a virtual environment with the neccesary dependencies:
```shell script
# Create a virtualenv and activate it
virtualenv -p $(which python3) venv
source venv/bin/activate

# Install the dependencies
pip install -r requirements.txt -r test-requirements.txt 

# Run the basic test suite (covers most of the pystardog functionalities)
pytest test/test_admin_basic.py test/test_connection.py test/test_utils.py -s 
```


## Format
To run a format of all the files
```shell script
virtualenv -p $(which python3) venv
. venv/bin/activate
pip install -r test-requirements.txt
black .
```

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
