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

from robcore.db.driver import DatabaseDriver
from robcore.controller.backend.base import WorkflowController
from robcore.model.workflow.state import StatePending

import robcore.config.engine as config
import robcore.controller.backend.sync as sync
import robcore.controller.io as fileio
import robcore.controller.run as runstore
import robcore.controller.serial as serial
import robcore.util as util


class MultiProcessWorkflowEngine(WorkflowController):
    """The workflow engine is used to execute workflow templates for a given
    set of arguments. Each workflow is executed as a serial workflow in a
    separate process.

    The multi process controller updates the underlying database when the state
    of an executed workflow changes. This class therefore does not implement the
    get_run_state method since the controlling workflow engine is never expected
    to call the method on this controller.
    """
    def __init__(self, base_dir=None, verbose=False):
        """Initialize the base directory under which all workflow runs are
        maintained. If the directory does not exist it will be created.

        Parameters
        ----------
        base_dir: string
            Path to directory on disk
        verbose: bool, optional
            Print command strings to STDOUT during workflow execution
        """
        # Set base directory and ensure that it exists
        if not base_dir is None:
            self.base_dir = util.create_dir(base_dir)
        else:
             self.base_dir = util.create_dir(config.ENGIN_BASEDIR())
        self.verbose = verbose
        # Dictionary of all running tasks
        self.tasks = dict()
        # Lock to manage asynchronous access to the task dictionary
        self.lock = Lock()

    def asynchronous_events(self):
        """The workflow controller will update the underlying database whenever
        the state of an executed workflow changes.

        Returns
        -------
        bool
        """
        return True

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
        template: robcore.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations
        arguments: dict(robcore.model.template.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        robcore.model.workflow.state.WorkflowState

        Raises
        ------
        robcore.error.DuplicateRunError
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
                run_workflow,
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

    def get_run_dir(self, run_id):
        """Get the path to directory that stores the run files.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return os.path.join(self.base_dir, run_id)

    def modify_template(self, workflow_spec, tmpl_parameters, add_parameters):
        """Modify a given workflow specification by adding the given parameters
        to a given set of template parameters.

        This function is dependent on the workflow specification syntax that is
        supported by a workflow engine.

        Returns the modified workflow specification and the modified parameter
        index. Raises an error if the parameter identifier in the resulting
        parameter index are no longer unique.

        Parameters
        ----------
        workflow_spec: dict
            Workflow specification
        tmpl_parameters: dict(robcore.model.template.parameter.base.TemplateParameter)
            Existing template parameters
        add_parameters: dict(robcore.model.template.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        dict, dict(robcore.model.template.parameter.base.TemplateParameter)

        Raises
        ------
        robcore.error.DuplicateParameterError
        robcore.error.InvalidTemplateError
        """
        return serial.modify_spec(
            workflow_spec=workflow_spec,
            tmpl_parameters=tmpl_parameters,
            add_parameters=add_parameters
        )

    def remove_run(self, run_id):
        """Remove all files and directories that belong to the run with the
        given identifier. This method does not verify that the task is in an
        inactive state. It is assume that the constraint has been checked by
        the caller.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        """
        run_dir = self.get_run_dir(run_id)
        if os.path.isdir(run_dir):
            shutil.rmtree(run_dir)
        with self.lock:
            # Remove the task if it is still in the index list. Here we do not
            # check the run state. We assume that it has been verified by the
            # caller that the task can be deleted.
            if run_id in self.tasks:
                pool = tasks[run_id]
                pool.close()
                del tasks[run_id]


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


def run_workflow(run_id, run_dir, state, input_files, output_files, commands, verbose):
    """Execute a list of commands synchronously. This is the worker function for
    asynchronous workflow executions. Starts by copying input files and then
    executes the workflow synchronously.

    Returns a tuple containing the task identifier and a serialization of the
    workflow state.

    Parameters
    ----------
    run_id: string
        Unique run identifier
    run_dir: string
        Path to the working directory of the workflow run
    state: robcore.model.workflow.state.WorkflowState
        Current workflow state (to access the timestamps)
    input_files: list((string, string))
        List of source,target path pairs for files that are being copied
    output_files: list(string)
        Relative path of output files that are generated by the workflow run
    commands: list(string)
        List of expanded commands from a template workflow specification
    verbose: bool, optional
        Output executed commands if flag is True

    Returns
    -------
    (string, dict)
    """
    try:
        fileio.copy_files(files=input_files, target_dir=run_dir)
        result_state = serial.run(
            run_dir=run_dir,
            commands=commands,
            output_files=output_files,
            verbose=verbose
        )
    except (OSError, IOError) as ex:
        result_state = state.error(messages=[str(ex)])
    return run_id, sync.serialize_state(result_state)
