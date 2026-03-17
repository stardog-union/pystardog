Stardog Cloud
###############

The :obj:`stardog.cloud` subpackage provides clients for accessing Stardog Cloud services, such as `Stardog Voicebox <https://stardog.ai/>`_.

Installation
************

To use Stardog Cloud functionality, install pystardog with the cloud optional dependencies:

.. code-block:: bash

    pip install pystardog[cloud]

.. list-table:: Clients
    :widths: 50 50
    :header-rows: 1

    * - Class
      - Description
    * - :class:`stardog.cloud.client.Client`
      - Ideal for simple blocking operations in traditional Python applications.
    * - :class:`stardog.cloud.client.AsyncClient`
      - Suitable for async programming models, such as ``asyncio``.

Voicebox Integration
************************

Access Stardog's Voicebox capabilities through an instance of :class:`stardog.cloud.voicebox.VoiceboxApp` class.

.. note::
   The :class:`stardog.cloud.voicebox.VoiceboxApp` provides both synchronous and asynchronous methods, offering the same functionality. Asynchronous methods are prefixed with `async_` and return coroutines that can be awaited, while synchronous methods return objects directly. For example, :obj:`stardog.cloud.voicebox.VoiceboxApp.async_ask` returns a coroutine, while :obj:`stardog.cloud.voicebox.VoiceboxApp.ask` returns a :class:`stardog.cloud.voicebox.VoiceboxAnswer`.



Synchronous usage
========================

.. code-block:: python

    from stardog.cloud.client import Client

    with Client() as client:
        app = client.voicebox_app(app_api_token="your-token", client_id="your-client-id")
        response = app.ask(question="Your question here")

Asynchronous usage
========================


.. code-block:: python

    from stardog.cloud.client import AsyncClient

    async with AsyncClient() as client:
        app = client.voicebox_app(app_api_token="your-token", client_id="your-client-id")
        response = await app.async_ask(question="Your question here")


Streaming
========================

For long-running questions, use the streaming API to receive incremental updates.
Each streamed answer is a :class:`~stardog.cloud.voicebox.VoiceboxAnswer` — the same model
returned by :obj:`~stardog.cloud.voicebox.VoiceboxApp.ask`. The ``pending`` field indicates
whether more answers are coming (``True``) or the stream is complete (``False``).

Synchronous streaming:

.. code-block:: python

    from stardog.cloud.client import Client

    with Client() as client:
        app = client.voicebox_app(app_api_token="your-token", client_id="your-client-id")

        with app.stream_ask(question="Your question here") as stream:
            for answer in stream:
                if not answer.pending:
                    print(answer.content)

Asynchronous streaming:

.. code-block:: python

    from stardog.cloud.client import AsyncClient

    async with AsyncClient() as client:
        app = client.voicebox_app(app_api_token="your-token", client_id="your-client-id")

        async with app.async_stream_ask(question="Your question here") as stream:
            async for answer in stream:
                if not answer.pending:
                    print(answer.content)


Error Handling
========================

The package raises custom exceptions defined in the :obj:`stardog.cloud.exceptions` module. These exceptions can be caught when interacting with the Stardog Cloud clients.

All clients raise exceptions if needed, with :class:`stardog.cloud.exceptions.StardogCloudException` serving as the base exception for any error raised.

.. code-block:: python

    from stardog.cloud.exceptions import (
        StardogCloudException,
        RateLimitExceededException
    )

    try:
        with Client() as client:
            app = client.voicebox_app(app_api_token="your-token", client_id="your-client-id")
            response = app.ask(question="Your question")
    except RateLimitExceededException as e:
        print(f"Rate limit exceeded: {e}")
    except StardogCloudException as e:
        print(f"Error occurred: {e}")



API Reference
*********************

stardog.cloud.client
============================

.. automodule:: stardog.cloud.client
    :members:
    :undoc-members:
    :show-inheritance:
    :special-members: __init__

stardog.cloud.voicebox
============================

.. automodule:: stardog.cloud.voicebox
    :members:
    :undoc-members:
    :show-inheritance:
    :special-members: __init__

stardog.cloud.exceptions
============================

.. automodule:: stardog.cloud.exceptions
    :members:
    :undoc-members:
    :show-inheritance:
    :special-members: __init__

