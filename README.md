# pystardog

[![PyPI version](https://badge.fury.io/py/pystardog.svg)](https://badge.fury.io/py/pystardog)

a Python wrapper for communicating with the Stardog HTTP server.

**Docs**: [http://pystardog.readthedocs.io](http://pystardog.readthedocs.io)

**Requirements**: Python 3.8+

## What is it?

This library wraps all the functionality of a client for the Stardog
Knowledge Graph, and provides access to a full set of functions such
as executing SPARQL queries and many administrative tasks.

The implementation uses the HTTP protocol, since most of Stardog
functionality is available using this protocol. For more information,
see [HTTP
Programming](https://docs.stardog.com/developing/http-api)
in Stardog's documentation.

## Installation

pystardog is on [PyPI](https://pypi.org/project/pystardog/). To install:

```shell
pip install pystardog
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

There is a Jupyter notebook and instructions in the [`notebooks`](./notebooks)
directory of this repository.

## Documentation

Documentation is available at [http://pystardog.readthedocs.io](http://pystardog.readthedocs.io)

### Build the Docs Locally

The docs can be built locally using [Sphinx](https://www.sphinx-doc.org/en/master/):

```shell
cd docs
pip install -r requirements.txt
make html
```

## Contributing and Development

Contrbutions are always welcome to pystardog.

To make a contribution:

1. Create a new branch off of `main`. There is no set naming convention for branches but try and keep it descriptive.

   ```bash
   git checkout -b feature/add-support-for-X
   ```

2. Make your changes. If you are making substantive changes to pystardog, tests should be added to ensure your changes are working as expected. See [Running Tests](#running-tests) for additional information
   about running tests.

3. Format your code. All Python code should be formatted using [Black](https://pypi.org/project/black/). See [Formatting Your Code](#formatting-your-code) for additional information.

4. Commit and push your code. Similar to branch names, there is no set structure for commit messages but try and keep your commit messages succinct and on topic.

   ```bash
   git commit -am "feat: adds support for feature X"
   git push origin feature/add-support-for-x
   ```

5. Create a pull request against `main`. All CircleCI checks should be passing in order to merge your PR. CircleCI will run tests against all supported versions of Python, single node and cluster tests for pystardog, as well as do some static analysis of the code.

### Running Tests

#### Requirements:

- [Docker](https://docs.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- Valid Stardog License

To run the tests locally, a valid Stardog license is required and placed at `dockerfiles/stardog-license-key.bin`.

1.  Bring a stardog instance using docker-compose. For testing about 90% of the pystardog features, just a single node is sufficient,
    although we also provide a cluster set up for further testing.

        ```shell
        # Bring a single node instance plus a bunch of Virtual Graphs for testing (Recommended).
        docker-compose -f docker-compose.single-node.yml up -d

        # A cluster setup is also provided, if cluster only features are to be implemented and tested.
        docker-compose -f docker-compose.cluster.yml up -d
        ```

2.  Create a virtual environment with the necessary dependencies:

    ```shell
    # Create a virtualenv and activate it
    virtualenv -p $(which python3) venv
    source venv/bin/activate

    # Install dependencies
    pip install -r requirements.txt -r test-requirements.txt
    ```

3.  Run the test suite:

    ```shell
    # Run the basic test suite (covers most of the pystardog functionalities)
    pytest test/test_admin_basic.py test/test_connection.py test/test_utils.py -s
    ```

    > **Note**
    > Tests can be targeted against a specific Stardog endpoint by specifying an `--endpoint` option to `pytest`. Please note, that the tests will make modifications
    > to the Stardog instance like deleting users, roles, databases, etc. By default, the `--endpoint` is set to `http://localhost:5820`,
    > which is where the Dockerized Stardog (defined in the Docker compose files) is configured to be available at.
    >
    > ```bash
    > pytest test/test_connection.py -k test_queries -s --endpoint https://my-other-stardog:5820
    > ```

### Formatting your code

To format all the Python code:

```shell
# make sure black is install
virtualenv -p $(which python3) venv
. venv/bin/activate
pip install -r test-requirements.txt

# run black formatter
black .
```
