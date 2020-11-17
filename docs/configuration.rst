=============
Configuration
=============


All components of the *Reproducible and Reusable Data Analysis Workflow Server* (**flowServ**) are configured using environment variables in an attempt to follow `The Twelve-Factor App methodology <https://12factor.net/>`_ for application development.



---------------
Web Service API
---------------

The **flowServ** Web service API base configuration is controlled by six environment variables: *FLOWSERV_API_DIR*, *FLOWSERV_API_HOST*, *FLOWSERV_API_NAME*, *FLOWSERV_API_PATH*, *FLOWSERV_API_PORT*, and *FLOWSERV_API_PROTOCOL*. Note that RESTful Web services that provide access to the API via HTTP requests may define additional configuration parameters.

The API maintains all files within (sub-folders) of a base directory on the file system. The base directory is specified using  the environment variable *FLOWSERV_API_DIR*. The default value is ``.flowserv``.

The API name, contained in the API service descriptor, is specified using the environment variable *FLOWSERV_API_NAME*. The default value is ``Reproducible and Reusable Data Analysis Workflow Server (API)``.

The base URL for all API resources is composed from the values in the environment variables *FLOWSERV_API_PROTOCOL*, *FLOWSERV_API_HOST*, *FLOWSERV_API_PORT*, and *FLOWSERV_API_PATH* (following the pattern protocol://host:port/path). If the port number is ``80`` it is omitted from the url pattern. The default values for the environment variables are ``http``, ``localhost``, ``5000``, and ``/flowserv/api/v1``, respectively.



--------------
Authentication
--------------

**flowServ** currently supports two modes of authentication. The selected mode is defined by the environment variable *FLOWSERV_AUTH*:

- OPEN: Defines an open-access policy to the API that does not require an authenticated user for API calls.
- DEFAULT: The default authentication policy requires a valid user identifier to be provided for API calls.

The environment variable *FLOWSERV_AUTH_TTL* is used to specify the time period (in milliseconds) for which an issued API key (used to authenticate users) is valid after a user login.



---------------
Workflow Engine
---------------

The **flowServ** API uses a workflow controller to handle execution of workflow templates. The interface for the controller ``WorkflowController`` is defined in the module ``flowserv.controller.base``. Different workflow engines will implement their own version of the controller. An instance of **flowServ** will currently use a single controller for the execution of all workflows. This controller is specified using the following two environment variables:

- *FLOWSERV_BACKEND_CLASS*: The name of the Python class that implements the workflow controller interface
- *FLOWSERV_BACKEND_MODULE*: The full name of the module that contains the implementation of the workflow controller interface

The specified controller module is imported dynamically. Each implementation of the workflow controller may define additional environment variables that are required for configuration.


Default Engine
--------------

By default, a simple multi-process engine is used that executes every workflow in a separate process. The environment settings for the default engine are as follows:

.. code-block:: console

    export FLOWSERV_BACKEND_MODULE=flowserv.controller.serial.engine
    export FLOWSERV_BACKEND_CLASS=SerialWorkflowEngine


Docker Engine
-------------

The environment settings for the Docker engine are as follows:

.. code-block:: console

    export FLOWSERV_BACKEND_MODULE=flowserv.controller.serial.docker
    export FLOWSERV_BACKEND_CLASS=DockerWorkflowEngine


Temporary Run Files
-------------------

The default engine and the Docker engine maintain run files in a temporary folder before they are moved to persistent storage (as defined by the file store parameters). The base folder for these temporary files can be configured using the environment variable *FLOWSERV_RUNSDIR*. If the variable is not set all workflow runs will use the sub-folder `runs` in the *FLOWSERV_API_DIR* as the default base directory.


--------
Database
--------

Database connections are established using the environment variable *FLOWSERV_DATABASE*. **flowServ** uses `SQLAlchemy <https://www.sqlalchemy.org/>`_ for the Object-Relational-Mapping and to access the underlying database. The value of *FLOWSERV_DATABASE* is passed to the SQLAlchemy engine at initialization. The value is expected to be a database connection URL. Consult the `SQLAlchemy Database Urls documentation <https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls>`_ for more information about the format of the URLs.


Connect to SQLite
-----------------

When using SQLite as the underlying database system, an example value for *FLOWSERV_DATABASE* is:

.. code-block:: bash

    export FLOWSERV_DATABASE=sqlite:////absolute/path/to/foo.db


Connect to PostgreSQL
---------------------


.. code-block:: bash

    export FLOWSERV_DATABASE=postgresql://scott:tiger@localhost/mydatabase


The following steps are an example for creating an initial empty database for **flowServ** in PostgreSQL:

.. code-block:: bash

    # Login as user postgres and connect to
    # the (local) database server
    sudo su - postgres
    psql -U postgres


.. code-block:: sql

    -- Create user flowserv with password flowserv
    CREATE USER flowserv;
    ALTER USER flowserv WITH PASSWORD 'flowserv';
    -- Create an empty database with owner flowserv
    CREATE DATABASE flowserv WITH OWNER flowserv;


If the environment variable *FLOWSERV_WEBAPP* is set to `True` scoped database sessions are used for web applications.


----------
File Store
----------

**flowServ** needs to store and access files for a variety of components and tasks. The files that are maintaind by the system include:

- static files that are associated with a workflow template,
- files that are uploaded by users as input to workflow runs, and
- result files of successful workflow runs.

By default, files are stored on the local file system in the directory that is specified by the *FLOWSERV_API_DIR* variable. Alternative storage backends can be configured using the environment variables *FLOWSERV_FILESTORE_CLASS* and *FLOWSERV_FILESTORE_MODULE*. These two variables are used to identify an existing implementation for the `flowserv.model.files.base.FileStore` interface. The package currently includes two implementations of the file store.


File System Store
-----------------

The default file store maintains all files in subfolders under the directory that is specified by the environment variable *FLOWSERV_API_DIR*. To configure this option set the environment variables as follows:

.. code-block:: base

    export FLOWSERV_FILESTORE_MODULE=flowserv.model.files.fs
    export FLOWSERV_FILESTORE_CLASS=FileSystemStore


S3 Bucket Store
---------------

The **S3 Bucket Store** allows storage of all files using `AWS Simple Cloud Storage (S3) <https://aws.amazon.com/s3/>`_. To configure this option set the environment variables as follows:


.. code-block:: base

    export FLOWSERV_FILESTORE_MODULE=flowserv.model.files.s3
    export FLOWSERV_FILESTORE_CLASS=BucketStore

This file store defines the additional environment variable *FLOWSERV_S3BUCKET*. This variable is used to get the unique identifier of the S3 storage bucket. During development when running test cases, the value of this variable should not be set.
