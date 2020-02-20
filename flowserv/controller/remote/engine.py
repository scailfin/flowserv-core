# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
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
import os
import time

from functools import partial
from multiprocessing import Lock, Pool

from flowserv.controller.base import WorkflowController
from flowserv.model.workflow.resource import FSObject
from flowserv.model.workflow.state import StateSuccess

import flowserv.controller.serial.engine as serial
import flowserv.core.util as util
import flowserv.model.workflow.state as serialize


"""Additional environment variables that control the configuration of the
remote workflow controller.
"""
REMOTE_ENGINE_ASYNC = 'REMOTE_ENGINE_ASYNC'
REMOTE_ENGINE_POLL = 'REMOTE_ENGINE_POLL'

# Default value for the poll interval.
DEFAULT_POLL_INTERVAL = '2'


class RemoteWorkflowController(WorkflowController):
    """Workflow controller that executes workflow templates for a given set of
    arguments using an external workflow engine. Each workflow is monitored by
    a separate process that continuously polls the workflow state.
    """
    def __init__(self, client, is_async=None):
        """Initialize the client that is used to interact with the remote
        workflow engine.

        Parameters
        ----------
        client: flowserv.controller.remote.client.RemoteClient
            Engine-specific implementation of the remote client that is used by
            the controller to interact with the workflow engine.
        is_async: bool, optional
            Flag that determines whether workflows execution is synchronous or
            asynchronous by default.
        """
        self.client = client
        # Set the is_async flag. If no value is given the default value is set
        # from the respective environment variable
        if is_async is not None:
            self.is_async = is_async
        else:
            val = os.environ.get(REMOTE_ENGINE_ASYNC, 'True')
            self.is_async = bool(val)
        # Dictionary of all running tasks. Maintains tuples containing the
        # multi-process pool object and the remote workflow identifier for
        # each run.
        self.tasks = dict()
        # Lock to manage asynchronous access to the task dictionary
        self.lock = Lock()

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
        with self.lock:
            # Ensure that the run has not been removed already
            if run_id in self.tasks:
                pool, workflow_id = self.tasks[run_id]
                # Close the pool and terminate any running processes
                if pool is not None:
                    pool.close()
                    pool.terminate()
                # Stop workflow execution at the engine. Ignore any errors that
                # may be raised.
                try:
                    self.client.stop_workflow(workflow_id)
                except Exception as ex:
                    logging.error(ex)
                    pass
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
        stime = os.environ.get(REMOTE_ENGINE_POLL, DEFAULT_POLL_INTERVAL)
        return [
            (REMOTE_ENGINE_ASYNC, str(self.is_async)),
            (REMOTE_ENGINE_POLL, str(stime))
        ]

    def exec_workflow(self, run, template, arguments, run_async=None):
        """Initiate the execution of a given workflow template for a set of
        argument values. This will start a new process that executes a serial
        workflow asynchronously. Returns the state of the workflow after the
        process is stated (the state will therefore be RUNNING).

        The set of arguments is not further validated. It is assumed that the
        validation has been performed by the calling code (e.g., the run service
        manager).

        If the state of the run handle is not pending, an error is raised.

        Parameters
        ----------
        run: flowserv.model.run.base.RunHandle
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations.
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template.
        run_async: bool, optional
            Flag to determine whether the worklfow execution will block the
            workflow controller or run asynchronously.

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState

        Raises
        ------
        flowserv.core.error.DuplicateRunError
        """
        # Get the run state. Ensure that the run is in pending state
        if not run.is_pending():
            raise RuntimeError("invalid run state '{}'".format(run.state))
        try:
            # Create a workflow on the remote engine. This will also upload all
            # necessary files to the remote engine. Workflow execution may not
            # be started (indicated by the state property of the returned
            # handle for the remote workflow.
            wf = self.client.create_workflow(run, template, arguments)
            workflow_id = wf.identifier
            # Run the workflow. Depending on the values of the is_async and
            # run_async flags the process will either block execution while
            # monitoring the workflow state or not.
            if serial.RUN_ASYNC(run_async=run_async, is_async=self.is_async):
                # Run workflow asynchronously in a separate process
                pool = Pool(processes=1)
                task_callback_function = partial(
                    callback_function,
                    lock=self.lock,
                    tasks=self.tasks
                )
                with self.lock:
                    self.tasks[run.identifier] = (pool, workflow_id)
                pool.apply_async(
                    run_workflow,
                    args=(
                        run.identifier,
                        run.rundir,
                        workflow_id,
                        wf.state,
                        wf.output_files,
                        self.client
                    ),
                    callback=task_callback_function
                )
                return wf.state
            else:
                # Run workflow synchronously. This will lock the calling thread
                # while waiting (i.e., polling the remote engine) for the
                # workflow execution to finish.
                _, state_dict = run_workflow(
                    run.identifier,
                    run.rundir,
                    workflow_id,
                    wf.state,
                    wf.output_files,
                    self.client
                )
                return serialize.deserialize_state(state_dict)
        except Exception as ex:
            # Set the workflow runinto an ERROR state
            logging.error(ex)
            return run.state.error(messages=util.stacktrace(ex))


# -- Helper Methods -----------------------------------------------------------

def callback_function(result, lock, tasks):
    """Callback function for executed tasks.Removes the task from the task
    index and updates the run state in the underlying database.

    Parameters
    ----------
    result: (string, dict)
        Tuple of task identifier and serialized state of the workflow run
    lock: multiprocessing.Lock
        Lock for concurrency control
    tasks: dict
        Task index of the backend
    """
    run_id, state_dict = result
    with lock:
        if run_id in tasks:
            result_state = serialize.deserialize_state(state_dict)
            # Close the pool and remove the entry from the task index
            pool, _ = tasks[run_id]
            pool.close()
            del tasks[run_id]
    # Get an instance of the API to update the run state.
    from flowserv.service.api import service
    try:
        with service() as api:
            api.runs().update_run(
                run_id=run_id,
                state=result_state
            )
    except Exception as ex:
        logging.error(ex)


def run_workflow(run_id, rundir, workflow_id, state, output_files, client):
    """Execute a given workflow remotely. This is the worker function for
    asynchronous remote workflow executions. Starts the workflow and polls the
    remote server continuously until execution is finished.

    Returns a tuple containing the task identifier and a serialization of the
    final workflow state.

    Parameters
    ----------
    run_id: string
        Unique run identifier.
    rundir: string
        Path to the working directory of the workflow run.
    state: flowserv.model.workflow.state.WorkflowState
        Current workflow state (to access the timestamps).
    workflow_id: string
        Unique identifier for the workflow on the remote engine.
    output_files: list(string)
        Relative path of output files that are generated by the workflow run.
    client: flowserv.controller.remote.client.RemoteClient
        Implementation of the remote client that is used to interact with the
        workflow engine.

    Returns
    -------
    (string, dict)
    """
    logging.debug('start run {}'.format(run_id))
    try:
        # Keep track of the state of the remote workflow.
        wf_state = state
        # Monitor the workflow state until the workflow is not in an active
        # state anymore.
        stime = os.environ.get(REMOTE_ENGINE_POLL, DEFAULT_POLL_INTERVAL)
        try:
            stime = float(stime)
        except ValueError:
            stime = float(DEFAULT_POLL_INTERVAL)
        while wf_state.is_active():
            time.sleep(stime)
            # Get the current workflow status
            curr_state = client.get_workflow_state(workflow_id, wf_state)
            if wf_state == curr_state:
                # Do nothing if the workflow status hasn't changed
                continue
            wf_state = curr_state
            if wf_state.is_running():
                # Get an instance of the API to update the run state.
                from flowserv.service.api import service
                try:
                    with service() as api:
                        api.runs().update_run(
                            run_id=run_id,
                            state=wf_state
                        )
                except Exception as ex:
                    logging.error(ex)
                    # Simulate an error response from the API
                    wf_state = wf_state.error(messages=[str(ex)])
        if wf_state.is_success():
            # Download the result files. The wf_state object is not expected
            # to contain the resource file information.
            files = list()
            for resource_name in output_files:
                # Download the respective result file first
                target = os.path.join(rundir, resource_name)
                client.download_file(workflow_id, resource_name, target)
                f = FSObject(
                    identifier=util.get_unique_identifier(),
                    name=resource_name,
                    filename=target
                )
                files.append(f)
            # Create a modified workflow state handle that contains the wrkflow
            # result resources.
            wf_state = StateSuccess(
                created_at=wf_state.created_at,
                started_at=wf_state.started_at,
                finished_at=wf_state.finished_at,
                resources=files
            )
    except Exception as ex:
        logging.error(ex)
        wf_state = state.error(messages=util.stacktrace(ex))
    logging.debug('finished run {} = {}'.format(run_id, wf_state.type_id))
    return run_id, serialize.serialize_state(wf_state)
