# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The abstract remote client class is used by the remote workflow controller
to interact with a workflow engine. Different workflow engines will implement
their own version of the remote client. The client provides the functionality
that is required by the workflow controller to execute workflows, cancel
workflow execution, get workflow status, and to download workflow result files.
"""

from abc import ABCMeta, abstractmethod


class RemoteClient(metaclass=ABCMeta):  # pragma: no cover
    """The remote client class is an abstract interface that defines the
    methods that are required by the remote workflow controller to execute and
    monitor remote workflows. Different workflow engies will implement their
    own version of the interface.
    """
    @abstractmethod
    def create_workflow(self, run, template, arguments):
        """Create a new instance of a workflow from the given workflow
        template and user-provided arguments. Implementations of this method
        will also upload any files to the remomote engine that are required to
        execute the workflow. A created workflow may not be running immediately
        but at minimum scheduled for execution. There is no separate signal to
        trigger execution start.

        Parameters
        ----------
        run: flowserv.model.base.RunObject
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations.
        arguments: dict
            Dictionary of argument values for parameters in the template.

        Returns
        -------
        flowserv.model.workflow.remote.RemoteWorkflowObject
        """
        raise NotImplementedError()

    @abstractmethod
    def download_file(self, workflow_id, source, target):
        """Download file from relative location at remote engine to a given
        target path.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier.
        source: string
            Relative path to source file in workflow workspace at the remote
            workflow engine.
        target: string
            Path to target file on local disk.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workflow_state(self, workflow_id, current_state):
        """Get information about the current state of a given workflow.

        Note, if the returned result is SUCCESS the workflow resource files may
        not have been initialized properly. This will be done by the workflow
        controller. The timestamps, however, should be set accurately.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        current_state: flowserv.model.workflw.state.WorkflowState
            Last known state of the workflow by the workflow controller

        Returns
        -------
        flowserv.model.workflw.state.WorkflowState
        """
        raise NotImplementedError()

    @abstractmethod
    def stop_workflow(self, workflow_id):
        """Stop the execution of the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        """
        raise NotImplementedError()
