=================
API Documentation
=================

The ``robcore`` package contains a default implementation of an API that is intended to be used by Web applications. The implementation is contained in the modules in ``robcore.service``.



Submitting Arguments for Benchmark Runs
=======================================

The API method to start an new run for a submission is implemented in the module ``robcore.service.run``. The method ``start_run()`` takes as one of the arguments a list of objects that represent the user-provided arguments for template parameters. Each object is expected to be a simple (flat) dictionary. Each dictionary is expected to contain an ``id`` and ``vaule`` element. The ``id`` references the parameter identifier. The ``value`` is dependent on the type of the parameter. At this point only strings and numbers are supported. For example,

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

