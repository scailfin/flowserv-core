# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the remote client for test purposes."""

import os

from flowserv.controller.remote.client import RemoteClient
from flowserv.controller.remote.engine import RemoteWorkflowController
from flowserv.model.workflow.remote import RemoteWorkflowObject
from flowserv.model.workflow.serial import SerialWorkflow
from flowserv.model.workflow.state import StatePending

import flowserv.util as util


class RemoteTestClient(RemoteClient):
    """Implementation of the remote workflow engine client. Simulates the
    execution of a workflow. The remote workflow initially is in pending state.
    The first call to the get_workflow_state method will return a workflow in
    running state without actually stating any workflow execution. The next N
    calls to get_workflow_state will then simulate a runnign workflow. When
    the method is then called next either successful run or an error run is
    returned.
    """
    def __init__(self, runcount=5, error=None, data=['no data']):
        """Initialize the internal state that maintains the created workflow.
        The client only supports execution for a single workflow at a time.

        Parameters
        ----------
        runcount: int, default=5
            Number of poll counts before the workflow state changes. This is
            used to simulate asynchronous workflow excution.
        error: string
            Error message. If given the resulting workflow run will be in
            error state and this string will be the only error message.
        data: list or dict, default=['no data']
            Result file content for successful workflow runs. Writes this data
            item to the result file when the poll counter reaches the run count
            and the error message is None.
        """
        self.runcount = runcount
        self.error = error
        self.data = data
        self.state = None
        # Count the number of times that the get_workflow_state() method has
        # been called.
        self._pollcount = None

    def create_workflow(self, run, template, arguments):
        """Create a new instance of a workflow from the given workflow
        template and user-provided arguments.

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
        # Create a serial workfow to have a workflow handle.
        wf = SerialWorkflow(template, arguments, sourcedir=None)
        self.state = StatePending()
        self._pollcount = 0
        return RemoteWorkflowObject(
            workflow_id=run.run_id,
            state=self.state,
            output_files=wf.output_files()
        )

    def download_file(self, workflow_id, source, target):
        """Download file from relative location in the base directory to a
        given target path. Since the workflow is executed in the run directory
        no files need to be copied.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier.
        source: string
            Relative path to source file in workflow workspace.
        target: string
            Path to target file on local disk.
        """
        os.makedirs(os.path.dirname(target), exist_ok=True)
        util.write_object(obj=self.data, filename=target)

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
        if self.state is None:
            raise ValueError('unknown workflow')
        # Set the workflow to running state if this is the first call to the
        # method.
        if self._pollcount == 0:
            self.state = self.state.start()
        elif self._pollcount > self.runcount:
            if self.error:
                self.state = self.state.error(messages=[self.error])
            else:
                self.state = self.state.success()
        self._pollcount += 1
        return self.state

    def stop_workflow(self, workflow_id):
        """Stop the execution of the workflow with the given identifier. Set
        the get state counter to a negative value to avoid that the workflow is
        executed.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        """
        self.state = None


class RemoteTestController(RemoteWorkflowController):
    """Extend remote workflow controller with dummy template modification
    method.
    """
    def __init__(self, client, poll_interval, is_async):
        """Initialize the test client.

        Parameters
        ----------
        client: flowserv.tests.remote.RemoteTestClient
            Test client.
        """
        super(RemoteTestController, self).__init__(
            client=client,
            poll_interval=poll_interval,
            is_async=is_async
        )
