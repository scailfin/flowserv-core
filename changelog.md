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


### 0.6.1 - 2020-11-20

* Drop support for Python 3.6.
* Rename run argument serialization helper functions.
