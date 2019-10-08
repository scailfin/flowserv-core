=================
API Documentation
=================


Submitting Arguments for Benchmark Runs
=======================================

Arguments for benchmark runs are serialized as a list of JSON objects that contain an ``id`` and ``vaule`` element. The ``id`` references the parameter identifier. The ``value`` is dependent on the type of the parameter. At this point only strings and numbers are supported. For example,

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

