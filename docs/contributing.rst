Contributing
============

Contributions are always welcome to pystardog.

Making a Contribution
**********************

To make a contribution:

1. **Create a new branch** off of ``main``. There is no set naming convention for branches but try and keep it descriptive.

   .. code-block:: bash

       git checkout -b feature/add-support-for-X

2. **Make your changes**. If you are making substantive changes to pystardog, tests should be added to ensure your changes are working as expected. See `Running Tests`_ for additional information about running tests.

3. **Format your code**. All Python code should be formatted using `Black <https://pypi.org/project/black/>`_. See `Formatting Your Code`_ for additional information.

4. **Commit and push your code**. Similar to branch names, there is no set structure for commit messages but try and keep your commit messages succinct and on topic.

   .. code-block:: bash

       git commit -am "feat: adds support for feature X"
       git push origin feature/add-support-for-x

5. **Create a pull request** against ``main``. All CircleCI checks should be passing in order to merge your PR. CircleCI will run tests against all supported versions of Python, single node and cluster tests for pystardog, as well as do some static analysis of the code.

Development Setup
*****************

Running Tests
-------------

**Requirements:**

- `Docker <https://docs.docker.com/>`_
- `Docker Compose <https://docs.docker.com/compose/>`_
- Valid Stardog License

To run the tests locally, a valid Stardog license is required and placed at ``dockerfiles/stardog-license-key.bin``.

1. **Bring a stardog instance** using docker-compose. For testing about 90% of the pystardog features, just a single node is sufficient, although we also provide a cluster set up for further testing.

   .. code-block:: shell

       # Bring a single node instance plus a bunch of Virtual Graphs for testing (Recommended).
       docker-compose -f docker-compose.single-node.yml up -d

       # A cluster setup is also provided, if cluster only features are to be implemented and tested.
       docker-compose -f docker-compose.cluster.yml up -d

2. **Install the package** in development mode with dependencies:

   .. code-block:: shell

       # Create a virtual environment and activate it
       python -m venv venv
       source venv/bin/activate

       # Install in development mode with dev dependencies
       pip install -e ".[dev]"

3. **Run the test suite**:

   .. code-block:: shell

       # Run the basic test suite (covers most of the pystardog functionalities)
       pytest test/test_admin_basic.py test/test_connection.py test/test_utils.py -s

   .. note::
      Tests can be targeted against a specific Stardog endpoint by specifying an ``--endpoint`` option to ``pytest``. Please note, that the tests will make modifications to the Stardog instance like deleting users, roles, databases, etc. By default, the ``--endpoint`` is set to ``http://localhost:5820``, which is where the Dockerized Stardog (defined in the Docker compose files) is configured to be available at.

      .. code-block:: bash

          pytest test/test_connection.py -k test_queries -s --endpoint https://my-other-stardog:5820

Formatting Your Code
--------------------

To format all the Python code:

.. code-block:: shell

    # Create and activate virtual environment
    python -m venv venv
    source venv/bin/activate
    pip install -e ".[dev]"

    # run black formatter
    black .

Running Tests with Tox
----------------------

To run tests across multiple Python versions:

.. code-block:: shell

    # Run tests for all supported Python versions
    tox

    # Run tests for a specific Python version
    tox -e py312

    # Run cluster-specific tests
    tox -e cluster

    # Run single-node-specific tests  
    tox -e single_node

Building Documentation
-----------------------

The docs can be built locally using `Sphinx <https://www.sphinx-doc.org/en/master/>`_:

.. code-block:: shell

    pip install -e ".[docs]"
    cd docs
    make html

Autodoc Type Hints
^^^^^^^^^^^^^^^^^^^

The docs use `sphinx-autodoc-typehints <https://github.com/tox-dev/sphinx-autodoc-typehints>`_ which allows you to omit types when documenting argument/returns types of functions. For example:

The following function:

.. code-block:: python

    def database(self, name: str) -> "Database":
        """Retrieves an object representing a database.

        :param name: The database name

        :return: the database
        """
        return Database(name, self.client)

will yield the following documentation after Sphinx processes it:

.. image:: https://github.com/stardog-union/pystardog/assets/23270779/f0defa61-e0d5-4df6-9daf-6842e41a3889

.. note::
   Only arguments that have an existing ``:param:`` directive in the docstring get their respective ``:type:`` directives added. The ``:rtype:`` directive is added if and only if no existing ``:rtype:`` is found. See the `docs <https://github.com/tox-dev/sphinx-autodoc-typehints>`_ for additional information on how the extension works.

Auto Build
^^^^^^^^^^^

Docs can be rebuilt automatically when saving a Python file by utilizing `sphinx-autobuild <https://github.com/executablebooks/sphinx-autobuild>`_

.. code-block:: shell

    pip install -e ".[docs]"
    cd docs
    make livehtml

This should make the docs available at http://localhost:8000.

Example output after running ``make livehtml``:

.. code-block:: text

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

Releasing
---------

Releases are published to PyPI by the ``release`` GitHub Actions workflow
(``.github/workflows/release.yml``) when a GitHub Release is published. The
workflow uses `PyPI Trusted Publishing <https://docs.pypi.org/trusted-publishers/>`_,
so there is no API token to hold, share, or rotate, and published artifacts carry
provenance attestations.

.. warning::

    Do not run ``twine upload`` by hand. Doing so bypasses the version check and
    the approval gate described below, and PyPI uploads are immutable -- a
    version that has been published can never be replaced, only yanked.

CircleCI continues to run the test suite and static analysis on every pull
request. The GitHub Actions workflow only builds and publishes. Trusted
publishing does not support CircleCI, which is why the release path lives in
GitHub Actions rather than alongside the tests.

Cutting a release
^^^^^^^^^^^^^^^^^

1. Update ``CHANGELOG.md``: rename the ``Unreleased`` section to the new version
   and add today's date.
2. Open a pull request bumping ``version`` in ``pyproject.toml``, and merge it.
3. Draft a `GitHub Release <https://github.com/stardog-union/pystardog/releases/new>`_
   against a new tag matching the version exactly, with no ``v`` prefix (for
   example ``0.21.0``). Use that version's ``CHANGELOG.md`` section as the body.
4. Review the draft, then publish it. Publishing starts the workflow.
5. Approve the deployment when the ``pypi`` environment requests a reviewer. The
   upload happens only after approval.

The workflow verifies that the release tag matches the version in
``pyproject.toml`` and fails before uploading if they disagree, so a mismatched
tag costs a re-run rather than a bad release.

Building locally
^^^^^^^^^^^^^^^^

To build and inspect the distribution without publishing it:

.. code-block:: shell

    # Install build dependencies
    pip install -e ".[build]"

    # Build the sdist and wheel
    python -m build

    # Validate the package metadata
    twine check --strict dist/*

The ``release`` workflow runs these same steps on every pull request, so
packaging problems surface during review rather than on release day.
