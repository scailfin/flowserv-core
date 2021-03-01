=================
API Documentation
=================


Authentication & Authorization
==============================

Authorization is handled by the API. At this point, API calls that access or manipulate submissions and submission runs require the user to be a member of the respective submission.



Submitting Arguments for Benchmark Runs
=======================================

The API method to start an new run for a submission takes as one of its arguments a list of objects that represent the user-provided values for template parameters. Each object is expected to be a simple (flat) dictionary. Each dictionary is expected to contain an ``name`` and ``value`` element. The ``name`` references the parameter identifier. The ``value`` is dependent on the type of the parameter. At this point only strings and numbers are supported. For example,

.. code-block:: json

    {
        "name": "parameter-id",
        "value": 1
    }


File Arguments
--------------

For parameters of type ``file`` the argument value is the identifier of a file that was uploaded by a previous API request. File arguments may have an additional element ``as`` to provide the target path for the file in the workflow execution environment. For example,

.. code-block:: json

    {
        "name": "parameter-id",
        "value": "file-id",
        "as": "data/myfile.txt"
    }
