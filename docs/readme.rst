About
=====

The *Reproducible Open Benchmarks for Data Analysis Platform (ROB)* is an experimental prototype for enabling community benchmarks of data analysis algorithms. This repository contains the implementation for the core infrastructure and API of ROB.

The goal of ROB is to allow benchmark participants to evaluate the performance of their algorithms in a controlled competition-style format. The overall architecture of ROB is shown below.

.. figure:: https://github.com/scailfin/rob-core/blob/master/docs/figures/architecture.png
   :scale: 50 %
   :alt: ROB Architecture

   **Overview of the ROB architecture.**


In ROB, the benchmark coordinator defines a workflow template along with input data. The template contains placeholders for workflow steps that are implemented by the benchmark participants (e.g., by providing Docker containers that satisfy the workflow steps). The ROB backend processes workflows on submission. Execution results are maintained in an internal database. The ROB user interface allows participants to submit new benchmark runs and to view the current leader board for the benchmark.



More Information
================

Workflow templates are motivated by the goal to allow users to run pre-defined data analytics workflows while providing their own input data, parameters, as well as their own code modules. Workflow templates are inspired by, but not limited to, workflow specifications for the `Reproducible Research Data Analysis Platform (REANA) <http://www.reanahub.io/>`_. The `Workflow Templates Section <https://github.com/scailfin/rob-core/blob/master/docs/workflow.rst>`_ provides further information about templates and their syntax. These templates are used by ROB to run benchmark workflows and maintain benchmark results.

The Reproducible Open Benchmarks API defines the main interface to programmatically interact with ROB. The API implementation in this repository provides a default serialization of all resources as Python dictionaries. The API is intended to be used by Web applications that (potentially) use  different frameworks. The `current default Web API implementation <https://github.com/scailfin/rob-wepapi-flask>`_ uses the `Flask web framework <https://flask.palletsprojects.com>`_.

There currently exist two different interfaces to interact with a running ROB Web API: the `Command Line Client <https://github.com/scailfin/rob-client>`_ and the `Web User Interface <https://github.com/scailfin/rob-ui>`_. See the respective repositories for further information on how to install and use these interfaces.



Note
====

This repository replaces `Workflow Templates <https://github.com/scailfin/benchmark-templates>`_ and the `Reproducible Benchmark Engine <https://github.com/scailfin/benchmark-engine>`_ from an earlier version of ROB.
