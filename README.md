# pystardog
[![PyPI version](https://badge.fury.io/py/pystardog.svg)](https://badge.fury.io/py/pystardog)

a Python wrapper for communicating with the Stardog HTTP server.

**Docs**: [http://pystardog.readthedocs.io](http://pystardog.readthedocs.io)

**Requirements**: Python 3.9+

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

For Stardog Cloud functionality:

```shell
pip install pystardog[cloud]
```

## Quick Examples

### Stardog Server (Local/Remote Endpoint)

Connect to a Stardog server instance to manage databases and execute queries:

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

### Stardog Cloud (Voicebox)

Use Stardog's cloud-based natural language query interface:

```python
import stardog.cloud

with stardog.cloud.Client() as client:
    voicebox = client.voicebox_app(
        app_api_token="your-voicebox-token",
        client_id="your-client-id"
    )
    answer = voicebox.ask("How many products were sold in 2023?")
    print(answer.content)
    print(f"SPARQL: {answer.sparql_query}")
```

## Interactive Tutorial

There is a Jupyter notebook and instructions in the [`notebooks`](./notebooks)
directory of this repository.

## Documentation

Documentation is available at [http://pystardog.readthedocs.io](http://pystardog.readthedocs.io)

### Build the Docs Locally

The docs can be built locally using [Sphinx](https://www.sphinx-doc.org/en/master/):

  ```shell
  pip install -e ".[docs]"
  cd docs
  make html
  ```

#### Autodoc Type Hints

The docs use [`sphinx-autodoc-typehints`](https://github.com/tox-dev/sphinx-autodoc-typehints) which allows you to omit types when documenting argument/returns types of functions. For example:

The following function:

```python
def database(self, name: str) -> "Database":
    """Retrieves an object representing a database.

    :param name: The database name

    :return: the database
    """
    return Database(name, self.client)
```

will yield the following documentation after Sphinx processes it:

![sphinx-autobuild-example](https://github.com/stardog-union/pystardog/assets/23270779/f0defa61-e0d5-4df6-9daf-6842e41a3889)

> **Note**
> Only arguments that have an existing `:param:` directive in the docstring get their
> respective `:type:` directives added. The `:rtype:` directive is added if and only if no existing `:rtype:` is found.
> See the [docs](https://github.com/tox-dev/sphinx-autodoc-typehints) for additional information on how the extension works.

#### Auto Build 

Docs can be rebuilt automatically when saving a Python file by utilizing [`sphinx-autobuild`](https://github.com/executablebooks/sphinx-autobuild)

```shell
pip install -e ".[docs]"
cd docs
make livehtml
```

This should make the docs available at [http://localhost:8000](http://localhost:8000).

Example output after running `make livehtml`:

```text
❯ make livehtml
sphinx-autobuild "." "_build"   --watch ../stardog/
[sphinx-autobuild] > sphinx-build /Users/frodo/projects/pystardog/docs /Users/frodo/projects/pystardog/docs/_build
Running Sphinx v6.2.1
loading pickled environment... done
building [mo]: targets for 0 po files that are out of date
writing output...
building [html]: targets for 0 source files that are out of date
updating environment: 0 added, 0 changed, 0 removed
reading sources...
looking for now-outdated files... none found
no targets are out of date.
build succeeded.

The HTML pages are in _build.
[I 230710 15:26:18 server:335] Serving on http://127.0.0.1:8000
[I 230710 15:26:18 handlers:62] Start watching changes
[I 230710 15:26:18 handlers:64] Start detecting changes
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

1. Bring a stardog instance using docker-compose. For testing about 90% of the pystardog features, just a single node is sufficient,
although we also provide a cluster set up for further testing. 

    ```shell
    # Bring a single node instance plus a bunch of Virtual Graphs for testing (Recommended).
    docker-compose -f docker-compose.single-node.yml up -d

    # A cluster setup is also provided, if cluster only features are to be implemented and tested.
    docker-compose -f docker-compose.cluster.yml up -d
    ```

2. Install the package in development mode with dependencies:

    ```shell
    # Create a virtual environment and activate it
    python -m venv venv
    source venv/bin/activate

    # Install in development mode with dev dependencies
    pip install -e ".[dev]"
    ```

3. Run the test suite:

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
  # Create and activate virtual environment
  python -m venv venv
  source venv/bin/activate
  pip install -e ".[dev]"

  # run black formatter
  black .
  ```

### Running Tests with Tox

To run tests across multiple Python versions:

```shell
# Run tests for all supported Python versions
tox

# Run tests for a specific Python version
tox -e py312

# Run cluster-specific tests
tox -e cluster

# Run single-node-specific tests  
tox -e single_node
```

### Building and Publishing

To build and publish the package to PyPI:

```shell
# Install build dependencies
pip install -e ".[build]"

# Build the package
python -m build

# Upload to PyPI (requires authentication)
twine upload dist/*
```

