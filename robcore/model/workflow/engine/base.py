# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Abstract interface for the workflow execution engine. The reproducible
benchmark engine is flexible with respect to the workflow engine that is being
used.

Each engine has to implement the abstract workflow engine class that is defined
in this module. The engine is responsible for interpreting a given workflow
template and a set of template parameter arguments. The engine is also
responsible for retrieving output files and for providing access to these files.
"""

from abc import abstractmethod


class WorkflowEngine(object):
    """The workflow engine is used to execute workflow templates for a given
    set of arguments for template parameters as well as to check the state of
    the workflow execution.

    Workflow executions, referred to as runs, are identified by unique run ids
    that are assigned by the engine when the execution starts.
    """
    @abstractmethod
    def cancel_run(self, run_id):
        """Request to cancel execution of the given run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        """
        raise NotImplementedError()

    @abstractmethod
    def execute(self, run_id, template, source_dir, arguments):
        """Initiate the execution of a given workflow template for a set of
        argument values. Returns the state of the workflow.

        The client provides a unique identifier for the workflow run that is
        being used to retrieve the workflow state in future calls.

        Parameters
        ----------
        run_id: string
            Unique identifier for the workflow run.
        template: robcore.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        source_dir: string
            Source directory that contains the static template files
        arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        robcore.model.workflow.state.base.WorkflowState
        """
        raise NotImplementedError()

    @abstractmethod
    def get_state(self, run_id):
        """Get the status of the workflow with the given identifier.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        robcore.model.workflow.state.base.WorkflowState

        Raises
        ------
        robcore.error.UnknownRunError
        """
        raise NotImplementedError()

    @abstractmethod
    def remove_run(self, run_id):
        """Clear internal resources for for the given run. Raises error if the
        run is still active.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        RuntimeError
        """
        raise NotImplementedError()
