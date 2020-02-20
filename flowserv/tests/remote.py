# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the remote client for test purposes."""

from flowserv.controller.remote.client import RemoteClient
from flowserv.controller.remote.engine import RemoteWorkflowController
from flowserv.controller.remote.workflow import RemoteWorkflowHandle
from flowserv.controller.serial.engine import run_workflow
from flowserv.controller.serial.workflow import SerialWorkflow

import flowserv.core.util as util
import flowserv.model.workflow.state as serialize


class RemoteTestClient(RemoteClient):
    """Implementation of the remote workflow engine client. Executes a workflow
    synchronously after a given number of calls to the get_workflow_state
    method. The workflow is in pending state after creation. The first call to
    the get_workflow_state method will return a workflow running state without
    actually stating workflow execution. The next call to get_workflow_state
    will then execute the workflow synchronously. This behavior allows to test
    the polling functionality of the remote engine controller with changes in
    the workflow state.
    """
    def __init__(self):
        """Initialize the internal state that maintains the created workflow.
        The client only supports execution for a single workflow at a time.
        """
        self.run = None
        self.serial_wf = None
        self.workflow = None

    def create_workflow(self, run, template, arguments):
        """Create a new instance of a workflow from the given workflow
        template and user-provided arguments. Implementations of this method
        will also upload any files to the remomote engine that are required to
        execute the workflow. A created workflow may not be running immediately
        but at minimum scheduled for execution. There is no separate signal to
        trigger execution start.

        Parameters
        ----------
        run: flowserv.model.run.base.RunHandle
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations.
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template.

        Returns
        -------
        flowserv.controller.remote.workflow.RemoteWorkflowHandle
        """
        # Assume a serial workflow template. Copy all input files to the run
        # directory.
        wf = SerialWorkflow(template, arguments)
        util.copy_files(files=wf.upload_files, target_dir=run.rundir)
        # Create top-level folder for all expected result files.
        util.create_directories(basedir=run.rundir, files=wf.output_files)
        self.serial_wf = wf
        self.run = run
        # Create a workflow handle but to not execute the workflow yet.
        # Execution starts when the get_workflow_state method is called for the
        # first time.
        self.workflow = RemoteWorkflowHandle(
            identifier=run.identifier,
            state=run.state,
            output_files=wf.output_files
        )
        return self.workflow

    def download_file(self, workflow_id, source, target):
        """Download file from relative location in the base directory to a
        given target path. SInce the workflow is executed in the run directory
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
        pass

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
        # The workfow is in pending state for the first call to this method.
        # Return running state without executing the workflow. If the workflow
        # is in running state the next time this method is called we execute
        # the workflow synchronously.
        state = self.workflow.state
        if state.is_pending():
            self.workflow.state = state.start()
        else:
            _, state_dict = run_workflow(
                run_id=self.run.identifier,
                rundir=self.run.rundir,
                state=state,
                output_files=self.serial_wf.output_files,
                steps=self.serial_wf.commands,
                verbose=False
            )
            self.workflow.state = serialize.deserialize_state(state_dict)
        return self.workflow.state

    def stop_workflow(self, workflow_id):
        """Stop the execution of the workflow with the given identifier. Set
        the get state counter to a negative value to avoid that the workflow is
        executed.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        """
        self.number_calls = -1


class RemoteTestController(RemoteWorkflowController):
    """Extend remote workflow controller with dummy template modification
    method.
    """
    def __init__(self):
        """Initialize the test client."""
        super(RemoteTestController, self).__init__(
            client=RemoteTestClient(),
            is_async=True
        )

    def modify_template(self, template, parameters):
        """Fake template modification. Returns the template as it is.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template handle.
        parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate
        """
        return template
