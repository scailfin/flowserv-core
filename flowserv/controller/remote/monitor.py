# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Monitor for remote workflow executions. The monitor is a separate thread
that continously polls the remote workflow engine to update the workflow state
in the local database.
"""

from typing import Dict, Optional
from threading import Thread

import logging
import time

from flowserv.controller.remote.client import RemoteWorkflowHandle
from flowserv.model.workflow.state import StateSuccess, WorkflowState
from flowserv.service.api import APIFactory

import flowserv.error as err
import flowserv.util as util


class WorkflowMonitor(Thread):
    """Thread that monitors execution of an external workflow. Polls the state
    of the workflow in regular intervals. Updates the local workflow state as
    the remote state changes.
    """
    def __init__(
        self, workflow: RemoteWorkflowHandle, poll_interval: float,
        service: APIFactory, tasks: Dict
    ):
        """Initialize the workflow information and the connection to the local
        service API.

        Parameters
        ----------
        workflow: flowserv.controller.remote.client.RemoteWorkflowHandle
            Handle for the monitored workflow.
        poll_interval: float
            Frequency (in sec.) at which the remote workflow engine is polled.
        service: contextlib,contextmanager, default=None
            Context manager to create an instance of the service API.
        tasks: dict
            Task dictionary that maps run identifier to remote workflow
            identifier.
        """
        Thread.__init__(self)
        self.workflow = workflow
        self.poll_interval = poll_interval
        self.service = service
        self.tasks = tasks

    def run(self):
        """Poll the remote server continuously until execution is finished."""
        try:
            monitor_workflow(
                workflow=self.workflow,
                poll_interval=self.poll_interval,
                service=self.service
            )
        except Exception as ex:
            logging.error(ex, exc_info=True)
            strace = util.stacktrace(ex)
            logging.debug('\n'.join(strace))
            state = self.workflow.state.error(messages=strace)
            if self.service is not None:
                with self.service() as api:
                    try:
                        api.runs().update_run(
                            run_id=self.workflow.run_id,
                            state=state,
                            runstore=self.workflow.runstore
                        )
                    except err.ConstraintViolationError:
                        pass
        # Remove the workflow information form the task list.
        try:
            del self.tasks[self.run_id]
        except Exception as ex:
            logging.error(ex, exc_info=True)
            logging.debug('\n'.join(util.stacktrace(ex)))


# -- Helper functions ---------------------------------------------------------

def monitor_workflow(
    workflow: RemoteWorkflowHandle, poll_interval: float, service: Optional[APIFactory] = None
) -> WorkflowState:
    """Monitor a remote workflow run by continuous polling at a given interval.
    Updates the local workflow state as the remote state changes.

    Returns the state of the inactive workflow and the temporary directory that
    contains the downloaded run result files. The run directory may be None for
    unsuccessful runs.

    Parameters
    ----------
    workflow: flowserv.controller.remote.client.RemoteWorkflowHandle
        Handle for the monitored workflow.
    poll_interval: float
        Frequency (in sec.) at which the remote workflow engine is polled.
    service: contextlib,contextmanager, default=None
        Context manager to create an instance of the service API.

    Returns
    -------
    flowserv.model.workflow.state.WorkflowState
    """
    logging.info('start monitoring workflow {}'.format(workflow.workflow_id))
    # Monitor the workflow state until the workflow is not in an active
    # state anymore.
    while workflow.is_active():
        time.sleep(poll_interval)
        # Get the current workflow status
        state = workflow.poll_state()
        if state is None:
            # Do nothing if the workflow status hasn't changed
            continue
        if state.is_success():
            # Create a modified workflow state handle that contains the
            # workflow result resources.
            state = StateSuccess(
                created_at=state.created_at,
                started_at=state.started_at,
                finished_at=state.finished_at,
                files=workflow.output_files
            )
        # Update the local state and the workflow state in the service API.
        if service is not None:
            try:
                with service() as api:
                    api.runs().update_run(
                        run_id=workflow.run_id,
                        state=state,
                        runstore=workflow.runstore
                    )
            except Exception as ex:
                # If the workflow is canceled for example, the state in the
                # API will have been changed and this may cause an error
                # here. If the remote workflow, however, remains active we
                # notify the remote engine to stop the workflow.
                logging.error('attempt to update run {}'.format(workflow.run_id))
                logging.error(ex, exc_info=True)
                if state.is_active():
                    try:
                        workflow.client.stop_workflow(workflow.workflow_id)
                    except Exception as ex:
                        logging.error(ex, exc_info=True)
                # Stop the thread by exiting the run method.
                return state
    msg = 'finished run {} = {}'.format(workflow.run_id, state.type_id)
    logging.info(msg)
    return state
