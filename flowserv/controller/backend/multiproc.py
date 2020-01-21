# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Simple implementation for a backend workflow controller. This controller
runs each workflow as a subprocess.

NOTE: This class is primarily intended for testing purposes. It is not
recommended to be used as the workflow backend in production systems.
"""

import os
import shutil

from functools import partial
from multiprocessing import Lock, Pool

from flowserv.core.db.driver import DatabaseDriver
from flowserv.controller.backend.base import BaseWorkflowController
from flowserv.model.workflow.state import StatePending

import flowserv.controller.backend.sync as sync
import flowserv.controller.io as fileio
import flowserv.controller.run as runstore
import flowserv.controller.serial as serial
import flowserv.core.error as err


class MultiProcessWorkflowEngine(BaseWorkflowController):
    """The workflow engine is used to execute workflow templates for a given
    set of arguments. Each workflow is executed as a serial workflow in a
    separate process.

    The multi process controller updates the underlying database when the state
    of an executed workflow changes. This class therefore does not implement the
    get_run_state method since the controlling workflow engine is never expected
    to call the method on this controller.
    """
    def __init__(self, base_dir=None, exec_func=None, verbose=False):
        """Initialize the base directory under which all workflow runs are
        maintained. If the directory does not exist it will be created.

        Parameters
        ----------
        base_dir: string
            Path to directory on disk
        run_workflow: func, optional
            Function that is used to execute the workflow commands
        verbose: bool, optional
            Print command strings to STDOUT during workflow execution
        """
        super(MultiProcessWorkflowEngine, self).__init__(
            base_dir=base_dir,
            is_async=True
        )
        self.exec_func = exec_func if exec_func is not None else run_workflow
        self.verbose = verbose
        # Dictionary of all running tasks
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
            Unique run identifier
        """
        with self.lock:
            # Ensure that the run has not been removed already
            if run_id in self.tasks:
                pool = self.tasks[run_id]
                # Close the pool and terminate any running processes
                if not pool is None:
                    pool.close()
                    pool.terminate()
                # Delete the task from the dictionary. The state of the
                # respective run will be updated by the workflow engine that
                # uses this controller for workflow execution
                del self.tasks[run_id]

    def exec_workflow(self, run_id, template, arguments):
        """Initiate the execution of a given workflow template for a set of
        argument values. This will start a new process that executes a serial
        workflow asynchronously. Returns the state of the workflow after the
        process is stated (the state will therefore be RUNNING).

        The set of arguments is not further validated. It is assumed that the
        validation has been performed by the calling code (e.g., the run service
        manager).

        Parameters
        ----------
        run_id: string
            Unique identifier for the workflow run that is used to reference
            the workflow in future calls
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(flowserv.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState

        Raises
        ------
        flowserv.core.error.DuplicateRunError
        """
        # Create run folder. If the folder exists we assume that the given run
        # identifier is not unique and raise an error.
        run_dir = self.get_run_dir(run_id)
        if os.path.isdir(run_dir):
            raise err.DuplicateRunError(run_id)
        os.makedirs(run_dir)
        # Expand template parameters. Get (i) list of files that need to be
        # copied, (ii) the expanded commands that represent the workflow steps,
        # and (iii) the list of output files.
        input_files = serial.upload_files(template, arguments)
        commands = serial.commands(template, arguments)
        output_files = serial.output_files(template, arguments)
        # Start a new process to run the workflow. Make sure to catch all
        # exceptions to set the run state properly
        state = StatePending()
        try:
            pool = Pool(processes=1)
            task_callback_function = partial(
                callback_function,
                lock=self.lock,
                tasks=self.tasks
            )
            state = state.start()
            with self.lock:
                self.tasks[run_id] = pool
            pool.apply_async(
                self.exec_func,
                args=(
                    run_id,
                    run_dir,
                    state,
                    input_files,
                    output_files,
                    commands,
                    self.verbose
                ),
                callback=task_callback_function
            )
        except Exception as ex:
            # Remove run directory if anything goes wrong while preparing the
            # workflow and starting the run. Set the workflow into an ERROR
            # state
            shutil.rmtree(run_dir)
            message = str(ex)
            state = state.error(messages=[message])
        # Return the workflow state
        return state


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

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
    with lock:
        run_id, state_dict = result
        if run_id in tasks:
            result_state = sync.deserialize_state(state_dict)
            # Close the pool and remove the entry from the task index
            pool = tasks[run_id]
            pool.close()
            del tasks[run_id]
            # Get a connection to the underlying database and update the run
            # state.
            with DatabaseDriver.get_connector().connect() as con:
                runstore.update_run(
                    con=con,
                    run_id=run_id,
                    state=result_state,
                    commit_changes=True
                )


def run_workflow(run_id, run_dir, state, input_files, output_files, steps, verbose):
    """Execute a list of workflow steps synchronously. This is the worker
    function for asynchronous workflow executions. Starts by copying input
    files and then executes the workflow synchronously.

    Returns a tuple containing the task identifier and a serialization of the
    workflow state.

    Parameters
    ----------
    run_id: string
        Unique run identifier
    run_dir: string
        Path to the working directory of the workflow run
    state: flowserv.model.workflow.state.WorkflowState
        Current workflow state (to access the timestamps)
    input_files: list((string, string))
        List of source,target path pairs for files that are being copied
    output_files: list(string)
        Relative path of output files that are generated by the workflow run
    steps: list(flowserv.model.template.command.Step)
        List of expanded workflow steps from a template workflow specification
    verbose: bool, optional
        Output executed command statements if flag is True

    Returns
    -------
    (string, dict)
    """
    try:
        fileio.copy_files(files=input_files, target_dir=run_dir)
        result_state = serial.run(
            run_dir=run_dir,
            steps=steps,
            output_files=output_files,
            verbose=verbose
        )
    except (OSError, IOError) as ex:
        result_state = state.error(messages=[str(ex)])
    return run_id, sync.serialize_state(result_state)
