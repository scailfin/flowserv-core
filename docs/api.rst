=================
API Documentation
=================

The ``robcore`` package contains a default implementation of the `ROB API for Web applications <https://raw.githubusercontent.com/scailfin/rob-core/master/dev/resources/api/v1/rob.yaml>`_`. The implementation is contained in the modules in ``robcore.service``.



Authentication & Authorization
==============================

The API itself does not handle user authentication. In the current `API specification <https://raw.githubusercontent.com/scailfin/rob-core/master/dev/resources/api/v1/rob.yaml>`_ we assume that HTTP requests contain a an access token in the request header field ``api_key``.

Authorization is handled by the API. At this point, API calls that access or manipulate submissions and submission runs require the user to be a member of the respective submission.



Submitting Arguments for Benchmark Runs
=======================================

The API method to start an new run for a submission takes as one of its arguments a list of objects that represent the user-provided values for template parameters. Each object is expected to be a simple (flat) dictionary. Each dictionary is expected to contain an ``id`` and ``value`` element. The ``id`` references the parameter identifier. The ``value`` is dependent on the type of the parameter. At this point only strings and numbers are supported. For example,

.. code-block:: json

    {
        "id": "parameter-id",
        "value": 1
    }


File Arguments
--------------

For parameters of type ``file`` the argument value is the identifier of a file that was uploaded by a previous API request. File arguments may have an additional element ``as`` to provide the target path for the file in the workflow execution environment. For example,

.. code-block:: json

    {
        "id": "parameter-id",
        "value": "file-id",
        "as": "data/myfile.txt"
    }
