# Reproducible and Reusable Data Analysis Workflow Server - Changelog

### 0.1.0 - 2020-02-14

* Initial Version (from rob-core).


### 0.1.1 - 2020-02-20

* Add generic controller for remote workflow engines.


### 0.1.2 - 2020-02-23

* Read template metadata from project description file when adding a workflow template to the repository.
* Create default names for templates with missing or non-unique names.
* Include short-cuts to default repositories when creating workflow templates using the command-line interface.


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


### 0.4.0 - 2020-09-??
