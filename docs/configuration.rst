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

The environment variable *FLOWSERV_AUTH_TTL* is used to specify the time period (in milliseconds) for which an issued API key is valid after a user login.



---------------
Workflow Engine
---------------

The **flowServ** API uses a workflow controller to handle execution of workflow templates. The interface for the controller ``WorkflowController`` is defined in the module ``flowserv.controller.base``. Different workflow engines will implement their own version of the controller. An instance of **flowServ** will currently use a single controller for the execution of all workflows. This controller is specified using the following two environment variables:

- *FLOWSERV_BACKEND_CLASS*: The name of the Pyhton class that implements the workflow controller interface
- *FLOWSERV_BACKEND_MODULE*: The full name of the module that contains the implementation of the workflow controller interface

The specified controller module is imported dynamically. Each implementation of the workflow controller may define additional environment variables that are required for configuration.



--------
Database
--------

Database connections are established using the environment variable *FLOWSERV_DBMS*  that determines the type of the database system that is used. **flowServ** currently supports the following two database systems: `SQLite <https://sqlite.org/index.html>`_ (identified by either ``SQLITE`` or ``SQLITE3``) and `PostgreSQL <https://www.postgresql.org/>`_ (identified by ``POSTGRES``, ``POSTGRESQL``, ``PSQL``, or ``PG``).

Depending on the specified database system additional environment variables are used to specify database connection parameter.


Connect to SQLite
-----------------

When using SQLite as the underlying database system, the environment variable *SQLITE_FLOWSERV_CONNECT* is expected to contain the path to the database file.


Connect to PostgreSQL
---------------------

When connecting to a PostgreSQL database server the database connection information is set using the following environment variables:

- *PG_FLOWSERV_HOST*: Address of the database host server (default: ``localhost``)
- *PG_FLOWSERV_DATABASE*: Name of the database (default: ``flowserv``)
- *PG_FLOWSERV_USER*: Database user name for authentication (default: ``flowserv``
- *PG_FLOWSERV_PASSWORD*: User password for authentication (default: ``flowserv``)
- *PG_FLOWSERV_PORT*: Connection port of database on host (default ``5432``)


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
