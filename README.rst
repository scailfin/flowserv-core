=======================================================
Reproducible Open Benchmarks for Data Analysis Platform
=======================================================

.. image:: https://img.shields.io/pypi/pyversions/benchmark-templates.svg
   :target: https://pypi.org/pypi/benchmark-templates

.. image:: https://api.travis-ci.org/scailfin/benchmark-templates.svg?branch=master
   :target: https://travis-ci.org/scailfin/benchmark-templates?branch=master

.. image:: https://codecov.io/gh/scailfin/benchmark-templates/branch/master/graph/badge.svg
 :target: https://codecov.io/gh/scailfin/benchmark-templates

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
   :target: https://github.com/scailfin/benchmark-templates/blob/master/LICENSE



About
=====

The *Reproducible Open Benchmarks for Data Analysis Platform (ROB)* is an experimental prototype for enabling community benchmarks of data analysis algorithms. This repository contains the implementation for the core infrastructure and API of ROB.

The goal of ROB is to allow benchmark participants to evaluate the performance of their algorithms in a controlled competition-style format. In ROB, the benchmark coordinator defines a workflow template along with input data. The template contains placeholders for workflow steps that are implemented by the benchmark participants (e.g., by providing Docker containers that satisfy the workflow steps). The ROB backend processes workflows on submission. Execution results are maintained in an internal database. The ROB user interface allows participants to submit new benchmark runs and to view the current leader board for the benchmark.

More Information
================

Workflow templates are motivated by the goal to allow users to run pre-defined data analytics workflows while providing their own input data, parameters, as well as their own code modules. Workflow templates are inspired by, but not limited to, workflow specifications for the `Reproducible Research Data Analysis Platform (REANA) <http://www.reanahub.io/>`_. The `Workflow Templates Section <https://github.com/scailfin/benchmark-templates/blob/master/docs/workflow.rst>`_ provides further information about templates and their syntax. These templates are used by ROB to run benchmark workflows and maintain benchmark results.

The **Reproducible Benchmarks API** defines the main interface to programmatically interact with ROB. The benchmark API is responsible for maintaining benchmark templates and benchmark runs. The results for all benchmark runs are integrated by the API to compile a leader board for each benchmark.

This repository replaces `Workflow Templates <https://github.com/scailfin/benchmark-templates>`_ and the `Reproducible Benchmark Engine <https://github.com/scailfin/benchmark-engine>`_ from an earlier version of ROB.
