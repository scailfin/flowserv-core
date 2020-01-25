=============
Configuration
=============


All components of the *Reproducible Open Benchmarks for Data Analysis Platform (ROB)* are configured using environment variables in an attempt to follow `The Twelve-Factor App methodology <https://12factor.net/>`_ for application development.


---
API
---

The following environment variables control the configuration of the **ROB API**. Note that the RESTful Web services that provide access to the API via HTTP requests can use additional configuration parameters.

The base directory where all API-related files are stored is specified using  the environment variable **FLOWSERV_API_DIR**. The default value is ``.rob``.

The API name, contained in the API service descriptor, is specified using the environment variable **FLOWSERV_API_NAME**. The default value is ``Reproducible Open Benchmarks for Data Analysis (API)``.

The base URL for all API resources is composed from the values in the environment variables **FLOWSERV_API_HOST**, **FLOWSERV_API_PORT**, and **FLOWSERV_API_PATH**. The default values are ``localhost``, ``5000``, and ``/rob/api/v1``, respectively.


--------------
Authentication
--------------

The environment variable **FLOWSERV_AUTH_TTL** is used to specify the time period (in milliseconds) for which an issued API key is valid after a user login.


--------
Database
--------

Database connections are established using the environment variable **FLOWSERV_DBMS**  that determines the type of the database system that is used. ROB currently supports the following two database systems: `SQLite <https://sqlite.org/index.html>`_ (identified by either ``SQLITE`` or ``SQLITE3``) and `PostgreSQL <https://www.postgresql.org/>`_ (identified by ``POSTGRES``, ``POSTGRESQL``, ``PSQL``, or ``PG``).

Depending on the specified database system additional environment variables are used to specify database connection parameter.


Connect to SQLite
-----------------

When using SQLite as the underlying database system, the environment variable **SQLITE_FLOWSERV_CONNECT** is expected to contain the path to the database file.


Connect to PostgreSQL
---------------------

When connecting to a PostgreSQL database server the database connection information is set using the following environment variables:

- **PG_FLOWSERV_HOST**: Address of the database host server (default: ``localhost``)
- **PG_FLOWSERV_DATABASE**: Name of the database (default: ``rob``)
- **PG_FLOWSERV_USER**: Database user name for authentication (default: ``rob``)
- **PG_FLOWSERV_PASSWORD**: User password for authentication (default: ``rob``)
- **PG_FLOWSERV_PORT**: Connection port of database on host (default ``5432``)


The following steps are an example for creating an initial empty database for ROB in PostgreSQL:

.. code-block:: bash
    # Login as user postgres and connect to the (local) database server
    sudo su - postgres
    psql -U postgres


.. code-block:: sql
    -- Create user rob with password rob
    CREATE USER rob;
    ALTER USER ds3 WITH PASSWORD 'rob';
    -- Create an empty database with owner rob
    CREATE DATABASE rob WITH OWNER rob;


---------------
Workflow Engine
---------------

The benchmark engine uses a workflow controller to handle execution of benchmark workflows. The interface for the controller is defined in ``flowserv.controller.base.WorkflowController``. Different workflow backends will implement their own version of the workflow controller. The specific controller that is to be used by an instance of ROB is specified using the following two environment variables:

- **FLOWSERV_ENGINE_CLASS**: The name of the Pyhton class that implements the workflow controller interface
- **FLOWSERV_ENGINE_MODULE**: The full name of the module that contains the implementation of the workflow controller interface

The specified controller module is imported dynamically. Each implementation of the workflow controller may define additional environment variables that are required for configuration.
