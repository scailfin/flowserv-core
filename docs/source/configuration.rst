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

- ``open``: Defines an open-access policy to the API that does not require an authenticated user for API calls.
- ``default``: The default authentication policy requires a valid user identifier to be provided for API calls.

The environment variable *FLOWSERV_AUTH_TTL* is used to specify the time period (in milliseconds) for which an issued API key (used to authenticate users) is valid after a user login.



---------------
Workflow Engine
---------------

The **flowServ** API uses a :class:WorkflowController to handle execution of workflow templates. Different workflow engines will implement their own version of the controller. An instance of **flowServ** will currently use a single controller for the execution of all workflows. This controller is specified using the following two environment variables:

- *FLOWSERV_BACKEND_CLASS*: The name of the Python class that implements the workflow controller interface
- *FLOWSERV_BACKEND_MODULE*: The full name of the module that contains the implementation of the workflow controller interface

The specified controller module is imported dynamically. Each implementation of the workflow controller may define additional environment variables that are required for configuration.

By default, a simple multi-process engine is used that executes every workflow in a separate process. The environment settings for the default engine are as follows:

.. code-block:: console

    export FLOWSERV_BACKEND_MODULE=flowserv.controller.serial.engine.base
    export FLOWSERV_BACKEND_CLASS=SerialWorkflowEngine


Serial Engine Workers
---------------------

When using the :class:SerialWorkflowEngine individual workflow steps can be executed by different workers (execution backends). **flowServ** currently supports execution using the Python ``subprocess`` package or the use of a Docker engine.

Engine workers are configured using a configuraton file (in Json or Yaml format) that specifies for Docker image identifier the execution backend. The format of the file is a list of entries as shown below:

.. code-block:: yaml

    - image: 'heikomueller/openclean-metanome:0.1.0'
      worker: 'docker'
      args:
          variables:
              jar: 'lib/Metanome.jar'
    - image: 'heikomueller/toptaggerdemo:0.2.0'
      worker: 'subprocess'

In the shown example workflow steps that use the Docker image `heikomueller/openclean-metanome:0.1.0` are executed using the :class:DockerWorker. The class receives the additional mapping of variables that is defined in the configuration as arguments when it is instantiated. Workflow steps that use the image `heikomueller/toptaggerdemo:0.2.0` will be executed as Python sub-processes.

Use the environment variable *FLOWSERV_SERIAL_WORKERS* to reference the configuration file for the engine workers. By default, all workflow steps will be executed as Python sub-processes if no configuration file is given.


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

**flowServ** needs to store and access files for a variety of components and tasks. The files that are maintained by the system include:

- static files that are associated with a workflow template,
- files that are uploaded by users as input to workflow runs, and
- result files of successful workflow runs.

By default, files are stored on the local file system in the directory that is specified by the *FLOWSERV_API_DIR* variable. Alternative storage backends can be configured using the environment variable *FLOWSERV_FILESTORE* that contains the configuration dictionary for the storage volume factory. The configuration object has to contain the mandatory element ``type`` that specifies the class of the storage volume that is used and the optional element ``name`` and ``args``. The ``name`` is used to identify the storage volume and the ``args`` element contains additional configuration parameters that are passed to the storage volume class constructor. **flowServ** currently supports four types of storage volumes.


File System Store
-----------------

The default file store maintains all files in subfolders under the directory that is specified by the environment variable *FLOWSERV_API_DIR*. To configure this option, used the following template:

.. code-block:: yaml

    "type": "fs"
    "args":
        "basedir": "path to the base directory on the file system"




Google Cloud File Store
-----------------------

The **Google Cloud Bucket** allows storage of all files using `Google Cloud File Store <https://cloud.google.com/filestore/>`_. The type identifier for this volume is ``gc``. The storage volume class has one additional configuration parameter to identify the storage bucket.

.. code-block:: yaml

    "type": "gc"
    "args":
        "bucket": "identifier of the storage bucket"


When using the Google Cloud Storage the Google Cloud credentials have to be configured. Set up authentication by creating a service account and setting the environment variable *GOOGLE_APPLICATION_CREDENTIALS*. See the `Cloud Storage Client Libraries documentation <https://cloud.google.com/storage/docs/reference/libraries#setting_up_authenticationcredentials>`_ for more details.

.. code-block:: bash

    export GOOGLE_APPLICATION_CREDENTIALS=[path-to-service-account-key-file]



S3 Bucket Store
---------------

The **S3 Bucket Store** allows storage of all files using `AWS Simple Cloud Storage (S3) <https://aws.amazon.com/s3/>`_. The type identifier for this volume is ``s3``. The storage volume class has one additional configuration parameter to identify the storage bucket.

.. code-block:: yaml

    "type": "s3"
    "args":
        "bucket": "identifier of the storage bucket"


When using the S3 storage volume the AWS credentials have to be configured. See the `AWS S3 CLI configuration documentation <https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html>`_ for more details.


SFTP File System Store
----------------------

**flowServ** also provides the option to store files on a remote file system and access them via ``sftp``. This storage volume is not recommended for storing workflow files. It's main purpose is to serve as a storage manager for copying files when executing workflow steps that run on remote maches (e.g., a HPC cluster). To configure the remote storage volume use the following configuration template.

.. code-block:: yaml

    "type": "sftp"
    "args":
        "hostname": "Name of the remote host"
        "port": post-number
        "sep": "path separator used by the remote file system [default: '/']"
        "look_for_keys": Boolean flag to enable searching for private key files [default=False]
