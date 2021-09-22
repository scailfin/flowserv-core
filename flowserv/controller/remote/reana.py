# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

from typing import Dict

from flowserv.controller.remote.client import RemoteClient, RemoteWorkflowHandle
from flowserv.controller.remote.engine import RemoteWorkflowController
from flowserv.model.base import RunObject
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.state import WorkflowState
from flowserv.service.api import APIFactory
from flowserv.volume.base import StorageVolume


class REANARemoteClient(RemoteClient):
    """Implementation for the remote client that interacts with a REANA
    cluster.
    """
    def __init__(self):
        """TODO: initialize the client."""
        pass

    def create_workflow(
        self, run: RunObject, template: WorkflowTemplate, arguments: Dict,
        staticfs: StorageVolume
    ) -> RemoteWorkflowHandle:
        """Create a new instance of a workflow from the given workflow
        template and user-provided arguments.

        The static storage volume provides access to static workflow template
        files that were created when the workflow template was installed.

        Implementations of this method will also upload any files to the
        remomote engine that are required to execute the workflow.

        A created workflow may not be running immediately but at minimum
        scheduled for execution. There is no separate signal to trigger
        execution start.

        The result is a handle to access the remote workflow object.

        Parameters
        ----------
        run: flowserv.model.base.RunObject
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations.
        arguments: dict
            Dictionary of argument values for parameters in the template.
        staticfs: flowserv.volume.base.StorageVolume
            Storage volume that contains the static files from the workflow
            template.

        Returns
        -------
        flowserv.controller.remote.client.RemoteWorkflowHandle
        """
        raise NotImplementedError()

    def get_workflow_state(
        self, workflow_id: str, current_state: WorkflowState
    ) -> WorkflowState:
        """Get information about the current state of a given workflow.

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

    def stop_workflow(self, workflow_id: str):
        """Stop the execution of the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        """
        raise NotImplementedError()


class REANAWorkflowController(RemoteWorkflowController):
    """Remote workflow controller that executes workflows on a REANA cluster
    using the REANA remote client.
    """
    def __init__(self, service: APIFactory):
        """TODO: Initialize the client. The service object is a dictionary that
        provides access to the environment variables. This could for example
        be used to define an environment variable REANA_CLIENT_POLL_INTERVAL
        to allow the user to configure the polling interval.

        Parameters
        ----------
        service: flowserv.service.api.APIFactory, default=None
            API factory for service callback during asynchronous workflow
            execution.
        """
        super(self, REANAWorkflowController).__init__(
            client=REANARemoteClient(),
            poll_interval=service.get('REANA_CLIENT_POLL_INTERVAL', 1.0),
            is_async=True,
            service=service
        )
