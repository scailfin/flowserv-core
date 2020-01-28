# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Abstract interface for the workflow controller. The controller provides
methods to start and cancel the execution of workflows, as well as methods that
poll the current state of a workflow.

The aim of an abstract workflow controller is to keep the workflow controller
flexible with respect to the processing backend that is being used. The
implementation of the controller can either orchestrate the execution of a
workflow iteself or be a wrapper around an existing workflow engine. An example
for latter is a workflow controller that wrapps around the REANA workflow
engine.

The implementation of the controller is responsible for interpreting a given
workflow template and a set of template parameter arguments. The controller
therefore requires a method for modifying the workflow template with a given
set of user-provided template modifiers.

The controller is also responsible for retrieving output files and for
providing access to these files.
"""

from abc import ABCMeta, abstractmethod


# -- Controller Interface -----------------------------------------------------

class WorkflowController(metaclass=ABCMeta):
    """The workflow controller is used to start execution of workflow templates
    for a given set of template parameter arguments, as well as to poll the
    state of workflow execution and to cancel execution.

    Workflow executions, referred to as runs, are identified by unique run ids
    that are assigned by components that are outside of the controller.
    Implementations of the controller are responsible for maintaining a mapping
    of these run identifiers to any indentifiers that are generated by the
    workflow engine.
    """
    @abstractmethod
    def cancel_run(self, run_id):
        """Request to cancel execution of the given run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        flowserv.core.error.UnknownRunError
        """
        raise NotImplementedError()

    @abstractmethod
    def configuration(self):
        """Get a list of tuples with the names of additional configuration
        variables and their current values.

        Returns
        -------
        list((string, string))
        """
        raise NotImplementedError()

    @abstractmethod
    def exec_workflow(self, run_id, template, arguments, run_async=True):
        """Initiate the execution of a given workflow template for a set of
        argument values. Returns the state of the workflow.

        The client provides a unique identifier for the workflow run that is
        being used to retrieve the workflow state in future calls.

        If the state of the run handle is not pending, an error is raised.

        Parameters
        ----------
        run: flowserv.model.run.base.RunHandle
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations.
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template.
        run_async: bool, optional
            Flag to determine whether the worklfow execution will block the
            workflow controller or run asynchronously.

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState
        """
        raise NotImplementedError()

    @abstractmethod
    def modify_template(self, template, parameters):
        """Modify the workflow specification in the given template by adding a
        set of parameters to the existing template parameter set.

        Returns a modified workflow template. Raises an error if the parameter
        identifier in the resulting template are no longer unique.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template handle.
        parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.core.error.DuplicateParameterError
        flowserv.core.error.InvalidTemplateError
        """
        raise NotImplementedError()
