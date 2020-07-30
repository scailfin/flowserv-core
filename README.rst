=======================================================
Reproducible and Reusable Data Analysis Workflow Server
=======================================================

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://github.com/scailfin/flowserv-core/blob/master/LICENSE

.. image:: https://github.com/scailfin/flowserv-core/workflows/build/badge.svg
    :target: https://github.com/scailfin/flowserv-core/actions?query=workflow%3A%22build%22

.. image:: https://codecov.io/gh/scailfin/flowserv-core/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/scailfin/flowserv-core



.. figure:: https://github.com/scailfin/flowserv-core/blob/master/docs/figures/logo.png
    :align: center
    :alt: flowServ Logo



About
=====

This repository contains the implementation of the core infrastructure for the *Reproducible and Reusable Data Analysis Workflow Server* (**flowServ**). This is an experimental prototype to support reuse and evaluation of published data analysis pipelines as well as community benchmarks of data analysis algorithms. **flowServ** is not yet-another workflow engine. The aim instead is to provide a layer between a client (e.g. a Web user interface) and a workflow engine to facilitate the execution of a defined workflow templates (as shown in the figure below). *flowServ* is designed to be independent of the underlying workflow engine.

Workflow templates contain placeholders for workflow steps and/or input data and parameters that are provided by the user (e.g., by providing Docker containers that satisfy the workflow steps or uploading input data files). **flowServ** triggers and monitors the execution of the workflow for the given input values and maintains the workflow results. The API provides the functionality to submit new workflow runs and to retrieve the evaluation results of completed workflow runs.


.. figure:: https://github.com/scailfin/flowserv-core/blob/master/docs/figures/flowserv-overview.png
    :align: center
    :alt: ROB Architecture



**flowServ** was motivated by the `Reproducible Open Benchmarks for Data Analysis Platform (ROB) <https://github.com/scailfin/rob-ui>`_.  The goal of ROB is to allow user communities to evaluate the performance of their different data analysis algorithms in a controlled competition-style format. In ROB, the benchmark coordinator defines the workflow template along with input data. Benchmark participants provide their own implementation of the variable workfow steps. The workflow engine processes workflows on submission. Execution results are maintained by **flowServ** in an internal database. The goal of **flowServ** is to be a more generic platform that can not only be used for benchmarks but also for other types of data analysis workflows.



More Information
================

Workflow templates are motivated by the goal to allow users to run pre-defined data analytics workflows while providing their own input data, parameters, as well as their own code modules. Workflow templates are inspired by, but not limited to, workflow specifications for the `Reproducible Research Data Analysis Platform (REANA) <http://www.reanahub.io/>`_. The `Workflow Templates Section <https://github.com/scailfin/flowserv-core/blob/master/docs/workflow.rst>`_ provides further information about templates and their syntax. These templates are used by **flowServ** to run workflows and to maintain benchmark results.

The **flowServ** API defines the main interface to programmatically interact with the underlying database and workflow engine. The API implementation that is included in this repository provides a default serialization of all API resources as Python dictionaries. The API is intended to be used by Web applications. These applications can be build using different frameworks. The `current default Web API implementation for ROB <https://github.com/scailfin/rob-webapi-flask>`_ uses the `Flask web framework <https://flask.palletsprojects.com>`_.

ROB currently provides two different interfaces to interact with a Web API: the `Command Line Client <https://github.com/scailfin/rob-client>`_ and the `Web User Interface <https://github.com/scailfin/rob-ui>`_. See the respective repositories for further information on how to install and use these interfaces.

For an overview of ROB there are `slides <https://github.com/scailfin/presentations/blob/master/slides/ROB-Demo-MSDSE2019.pdf>`_ from the ROB Demo at the `Moore-Sloan Data Science Environment's annual summit 2019 <https://sites.google.com/msdse.org/summit2019/home>`_ and our `presentation <https://indico.cern.ch/event/822074/contributions/3471463/attachments/1865533/3067815/Reproducible_Benchmarks_for_Data_Analysis-v3.pdf>`_ at the `Analysis Systems Topical Workshop <https://indico.cern.ch/event/822074/>`_.


Note
====

**flowServ** originated from the Reproducible Open Benchmarks for Data Analysis Platform (ROB). This repository replaces `Workflow Templates <https://github.com/scailfin/benchmark-templates>`_ and the `Reproducible Benchmark Engine <https://github.com/scailfin/benchmark-engine>`_ from an earlier version of ROB.
