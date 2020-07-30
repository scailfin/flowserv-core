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


### 0.2.1 - 2020-07-31

* Fix bug in created_at timestamp for workflow runs.
