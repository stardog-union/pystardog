Getting Started
===============

pystardog is a Python wrapper for communicating with the Stardog HTTP server, providing access to a full set of functions for executing SPARQL queries and administrative tasks.

**Requirements**: Python 3.9+

What is it?
***********

pystardog makes it easy to work with Stardog knowledge graphs from Python. Whether you're connecting to local or remote Stardog servers, or using Stardog Cloud's public API for features like AI-powered Voicebox, pystardog provides a simple, Pythonic interface to all of Stardog's capabilities.

The implementation uses the HTTP protocol to communicate with Stardog servers. For more information about the underlying API, see `HTTP Programming <https://docs.stardog.com/developing/http-api>`_ in Stardog's documentation.

Installation
************

pystardog is on `PyPI <https://pypi.org/project/pystardog/>`_. To install:

.. code-block:: shell

    pip install pystardog

For `Stardog Cloud <https://cloud.stardog.com>`_ functionality:

.. code-block:: shell

    pip install pystardog[cloud]

Quick Examples
**************

Stardog Server (Local/Remote Endpoint)
---------------------------------------

Connect to a Stardog server instance to manage databases and execute queries:

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

Stardog Cloud (Voicebox)
-------------------------

Use Voicebox to ask questions to your Stardog knowledge graph in natural language. Voicebox translates your questions into SPARQL queries, allowing you to interact with your data without needing to write SPARQL directly. This requires a Voicebox API token, which you can obtain from the Stardog Cloud console. Your requests will be sent to Stardog Cloud's Voicebox service, which processes the natural language input and returns the corresponding SPARQL query and results.

For more details, see the `Stardog Cloud API documentation <https://cloud.stardog.com/api/v1/docs>`_.

.. code-block:: python

    import stardog.cloud

    with stardog.cloud.Client() as client:
        voicebox = client.voicebox_app(
            app_api_token="your-voicebox-token",
            client_id="your-client-id"
        )
        answer = voicebox.ask("How many products were sold in 2023?")
        print(answer.content)
        print(f"SPARQL: {answer.sparql_query}")

