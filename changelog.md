# Reproducible and Reusable Data Analysis Workflow Server - Changelog

### 0.1.0 - 2020-02-14

* Initial Version (from rob-core).


### 0.1.1 - 2020-02-20

* Add generic controller for remote workflow engines.


### 0.1.2 - 2020-02-23

* Read template metadata from project description file when adding a workflow template to the repository.
* Create default names for templates with missing or non-unique names.
* Include Short cuts to default repositories when creating workflow templates using the command-line interface.


### 0.2.0 - 2020-07-30

* Use SQLAlchemy as ORM for the database model.
* Run workflow templates from command-line for test purposes.
* Add methods for running and testing workflows in Python scripts (or Jupyter Notebooks).
* Include support and a command line interface for single-workflow applications.
* Install workflow templates from global repository.


### 0.2.1 - 2020-08-01

* Fix bug in created_at timestamp for workflow runs.
* Command-line interface to register new users.


### 0.3.0 - 2020-08-25

* Option to add specification of output file properties for display purposes in workflow specification.
* Rename data type element in parameter declarations and result schema columns to 'dtype'.
* Add manifest file option as parameter for test workflow runs in Jupyter.
* Test workflow runs using Docker engine.


### 0.3.1 - 2020-08-27

* Commit changes after run state update (issue \#48).


### 0.3.2 - 2020-08-28

* Allow Conditional Parameter Replacements (\#50).


### 0.3.3 - 2020-08-28

* Handle optional manifest element in workflow repository entries (\#52).


### 0.4.0 - 2020-09-16

* Refactor code to include abstract file store for alternative storage backends (\#54).
* Implement file store for S3 buckets.
* Avoid redundant storage of static files for workflow runs (\#55).
* Remove old workflow runs from database (\#9).


### 0.5.0 - 2020-10-01

* Allow user-defined workflow and group identifier (\#59).
* Merge `flowserv` and `flowapp` console scripts.
* User-defined keys for output files (\#60).
* Add functionality for asynchronous runs to flowserv application object.
* Change flowserv application run result object (\#61).
* Configure authentication policy via environment variables.
* Create separate groups for each workflow run in the app (\#62).
* RunResults `get_file` returns a single file object (\#63).
* Flowserv class for running workflows in notebook environments (\#64, \#65).


### 0.5.1 - 2020-10-03

* Create directory for SQLite database files (\#68).
* Type tests for enum and numeric parameters.
* Add serialized file handle to file listing in the application run result.
* Fix bug in run handle serialization for output files.
* Fix bug in Docker engine (return rundir in error case).
* Add ignore postproc option to `flowserv install` (\#69).
* File object for files that are uploaded as part of Flask requests.
* Make database web_app flag configurable using environment variable (\#70).


### 0.6.0 - 2020-11-17

* Add parameter declarations for lists and records (\#73).
* Rename run argument serialization helper functions.


### 0.7.0 - 2021-01-20

* Introduce `flowserv.service.api.APIFactory` to support multiple clients with different configuration on the same machine.
* Add functionality for user groups and authentication via the API client.
* Better support for accessing run result files via the API client.
* Include full functionality from [rob-client](https://github.com/scailfin/rob-client) into the `flowserv` CLI (\#72).
* Provide support for local and remote API's.
* Remove some of the obsolete code for running workflows in Jupyter Notebooks (now included in the new client interface `flowserv.client.Flowserv`).
* Drop support for Python 3.6.


### 0.7.1 - 2021-01-29

* Add option for passing an access token in API factory call.
* Ensure not to override *FLOWSERV_ASYNC* in `ClientAPI`.
* Add CLI environment context to support entry points for `flowserv` and `rob`.
* Extend serialized objects to contain additional resources (i.e., groups and runs) for authenticated users.


### 0.7.2 - 2021-02-03

* Change the location of the default data directory to be the OS-specific user cache directory.


### 0.8.0 - 2021-03-01

* Remove additional sub-directory `.flowserv` from default data directory path.
* New workflow template parameter type `actor` to support complete workflow steps as argument values.
* Serial engine workers to support execution of individual workflow steps using different backends.
* Remove separate Docker workflow controller


## 0.8.1 - 2021-05-03

* Fix issue when creating database object on Windows.
* Fix issue with schema validator on Windows.
* Fix issue with bucket store unit tests on Windows.
* Fix unit test issues for Windows.
* Fix issue with subprocess worker on Windows.
