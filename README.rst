=======================================================
Reproducible Open Benchmarks for Data Analysis Platform
=======================================================

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://github.com/scailfin/rob-core/blob/master/LICENSE

.. image:: https://github.com/scailfin/rob-core/workflows/build/badge.svg
    :target: https://github.com/scailfin/rob-core/actions?query=workflow%3A%22build%22



About
=====

The *Reproducible Open Benchmarks for Data Analysis Platform (ROB)* is an experimental prototype for enabling community benchmarks of data analysis algorithms. This repository contains the implementation for the core infrastructure and API of ROB.

The goal of ROB is to allow user communities to evaluate the performance of their different data analysis algorithms in a controlled competition-style format. The overall architecture of ROB is shown below.

.. figure:: https://github.com/scailfin/rob-core/blob/master/docs/figures/architecture-small.png
    :align: center
    :alt: ROB Architecture

    **Overview of the ROB architecture.**


In ROB, the benchmark coordinator defines a workflow template along with input data. The template contains placeholders for workflow steps that are implemented by the benchmark participants (e.g., by providing Docker containers that satisfy the workflow steps). The ROB backend processes workflows on submission. Execution results are maintained in an internal database. The ROB user interface allows participants to submit new benchmark runs and to view the results of completed benchmark runs.



More Information
================

Workflow templates are motivated by the goal to allow users to run pre-defined data analytics workflows while providing their own input data, parameters, as well as their own code modules. Workflow templates are inspired by, but not limited to, workflow specifications for the `Reproducible Research Data Analysis Platform (REANA) <http://www.reanahub.io/>`_. The `Workflow Templates Section <https://github.com/scailfin/rob-core/blob/master/docs/workflow.rst>`_ provides further information about templates and their syntax. These templates are used by ROB to run benchmark workflows and to maintain benchmark results.

The Reproducible Open Benchmarks API defines the main interface to programmatically interact with ROB. The API implementation that is included in this repository provides a default serialization of all API resources as Python dictionaries. The API is intended to be used by Web applications. These applications can be build using different frameworks. The `current default Web API implementation <https://github.com/scailfin/rob-webapi-flask>`_ uses the `Flask web framework <https://flask.palletsprojects.com>`_.

There currently exist two different interfaces to interact with a ROB Web API: the `Command Line Client <https://github.com/scailfin/rob-client>`_ and the `Web User Interface <https://github.com/scailfin/rob-ui>`_. See the respective repositories for further information on how to install and use these interfaces.


For an overview of ROB there are `slides <https://github.com/scailfin/presentations/blob/master/slides/ROB-Demo-MSDSE2019.pdf>`_ from the ROB Demo at the `Moore-Sloan Data Science Environment's annual summit 2019 <https://sites.google.com/msdse.org/summit2019/home>`_ and our `presentation <https://indico.cern.ch/event/822074/contributions/3471463/attachments/1865533/3067815/Reproducible_Benchmarks_for_Data_Analysis-v3.pdf>`_ at the `Analysis Systems Topical Workshop <https://indico.cern.ch/event/822074/>`_.


Note
====

This repository replaces `Workflow Templates <https://github.com/scailfin/benchmark-templates>`_ and the `Reproducible Benchmark Engine <https://github.com/scailfin/benchmark-engine>`_ from an earlier version of ROB.
