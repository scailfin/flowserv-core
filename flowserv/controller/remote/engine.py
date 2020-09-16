# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation for a workflow controller backend that uses an external
(remote) workflow engine (e.g., an existing REANA cluster) for workflow
execution. The controller provides functionality for workflow creation, start,
stop, and monitpring using an (abstract) client class. For different types of
workflow engines only the RemoteClient class needs to be implements.
"""

import logging

from flowserv.controller.base import WorkflowController

import flowserv.config.controller as ctrl
import flowserv.controller.remote.config as config
import flowserv.controller.remote.monitor as monitor
import flowserv.util as util


class RemoteWorkflowController(WorkflowController):
    """Workflow controller that executes workflow templates for a given set of
    arguments using an external workflow engine. Each workflow is monitored by
    a separate process that continuously polls the workflow state.
    """
    def __init__(self, client, poll_interval=None, is_async=None):
        """Initialize the client that is used to interact with the remote
        workflow engine.

        Parameters
        ----------
        client: flowserv.controller.remote.client.RemoteClient
            Engine-specific implementation of the remote client that is used by
            the controller to interact with the workflow engine.
        poll_interval: int or float, default=None
            Frequency (in sec.) at which the remote workflow engine is polled.
        is_async: bool, optional
            Flag that determines whether workflows execution is synchronous or
            asynchronous by default.
        """
        self.client = client
        self.poll_interval = config.POLL_INTERVAL(value=poll_interval)
        self.is_async = ctrl.ENGINE_ASYNC(value=is_async)
        # Dictionary of all running tasks. Maintains tuples containing the
        # multi-process pool object and the remote workflow identifier for
        # each run.
        self.tasks = dict()

    def cancel_run(self, run_id):
        """Request to cancel execution of the given run. This method is usually
        called by the workflow engine that uses this controller for workflow
        execution. It is threfore assumed that the state of the workflow run
        is updated accordingly by the caller.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        """
        # Ensure that the run has not been removed already
        if run_id in self.tasks:
            workflow_id = self.tasks[run_id]
            # Stop workflow execution at the engine. Ignore any errors that
            # may be raised.
            try:
                self.client.stop_workflow(workflow_id)
            except Exception as ex:
                logging.error(ex)
                logging.debug('\n'.join(util.stacktrace(ex)))
            # Delete the task from the dictionary. The state of the
            # respective run will be updated by the workflow engine that
            # uses this controller for workflow execution
            del self.tasks[run_id]

    def configuration(self):
        """Get a list of tuples with the names of additional configuration
        variables and their current values.

        Returns
        -------
        list((string, string))
        """
        return [
            (ctrl.FLOWSERV_ASYNC, str(self.is_async)),
            (config.FLOWSERV_POLL_INTERVAL, str(self.poll_interval))
        ]

    def exec_workflow(self, run, template, arguments, service=None):
        """Initiate the execution of a given workflow template for a set of
        argument values. This will start a new process that executes a serial
        workflow asynchronously. Returns the state of the workflow after the
        process is stated (the state will therefore be RUNNING).

        The set of arguments is not further validated. It is assumed that the
        validation has been performed by the calling code (e.g., the run
        service manager).

        If the state of the run handle is not pending, an error is raised.

        Parameters
        ----------
        run: flowserv.model.base.RunHandle
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations.
        arguments: dict
            Dictionary of argument values for parameters in the template.
        service: contextlib,contextmanager, default=None
            Context manager to create an instance of the service API.

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState

        Raises
        ------
        flowserv.error.DuplicateRunError
        """
        # Get the run state. Ensure that the run is in pending state
        if not run.is_pending():
            raise RuntimeError("invalid run state '{}'".format(run.state()))
        try:
            # Raise an error if the service manager is not given.
            if service is None:
                raise ValueError('service manager not given')
            # Create a workflow on the remote engine. This will also upload all
            # necessary files to the remote engine. Workflow execution may not
            # be started (indicated by the state property of the returned
            # handle for the remote workflow).
            wf = self.client.create_workflow(
                run=run,
                template=template,
                arguments=arguments
            )
            workflow_id = wf.workflow_id
            # Run the workflow. Depending on the values of the is_async and
            # run_async flags the process will either block execution while
            # monitoring the workflow state or not.
            if self.is_async:
                self.tasks[run.run_id] = workflow_id
                # Start monitor tread for asynchronous monitoring.
                monitor.WorkflowMonitor(
                    run_id=run.run_id,
                    state=wf.state,
                    workflow_id=workflow_id,
                    output_files=wf.output_files(),
                    client=self.client,
                    poll_interval=self.poll_interval,
                    service=service,
                    tasks=self.tasks
                ).start()
                return wf.state, None
            else:
                # Run workflow synchronously. This will lock the calling thread
                # while waiting (i.e., polling the remote engine) for the
                # workflow execution to finish.
                state, rundir = monitor.monitor_workflow(
                    run_id=run.run_id,
                    state=wf.state,
                    workflow_id=workflow_id,
                    output_files=wf.output_files(),
                    client=self.client,
                    poll_interval=self.poll_interval
                )
                return state, rundir
        except Exception as ex:
            # Set the workflow runinto an ERROR state
            logging.error(ex)
            strace = util.stacktrace(ex)
            logging.debug('\n'.join(strace))
            return run.state().error(messages=strace), None
