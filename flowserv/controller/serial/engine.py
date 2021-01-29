# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation for a workflow controller backend that is capable of running
serial workflow specification. This controller allows execution in workflow
steps within separate sub-processes.

All workflow run files will be maintained in a (temporary) directory on the
local file system. The base folder for these run files in configured using the
environment variable FLOWSERV_RUNSDIR.
"""

from functools import partial
from multiprocessing import Lock, Pool
from typing import Callable, Optional

import logging
import os
import subprocess

from flowserv.config import FLOWSERV_ASYNC, FLOWSERV_BASEDIR, FLOWSERV_RUNSDIR, DEFAULT_RUNSDIR
from flowserv.controller.base import WorkflowController
from flowserv.model.files.factory import FS
from flowserv.model.workflow.serial import SerialWorkflow
from flowserv.service.api import APIFactory

import flowserv.error as err
import flowserv.util as util
import flowserv.model.workflow.state as serialize


class SerialWorkflowEngine(WorkflowController):
    """The workflow engine is used to execute workflow templates for a given
    set of arguments. Each workflow is executed as a serial workflow. The
    individual workflow steps can be executed in a separate process on request.
    """
    def __init__(
        self, service: APIFactory, exec_func: Optional[Callable] = None
    ):
        """Initialize the function that is used to execute individual workflow
        steps. The run workflow function in this module executes all steps
        within sub-processes in the same environment as the workflow
        controller.

        NOTE: Using the provided execution function is intended for development
        and private use only. It is not recommended (and very dangerous) to
        use this function in a public setting.

        Parameters
        ----------
        service: flowserv.service.api.APIFactory, default=None
            API factory for service callbach during asynchronous workflow
            execution.
        exec_func: callable, default=None
            Function that is used to execute the workflow commands
        """
        self.fs = FS(env=service)
        self.service = service
        self.exec_func = exec_func if exec_func is not None else run_workflow
        # The is_async flag controlls the default setting for asynchronous
        # execution. If the flag is False all workflow steps will be executed
        # in a sequentiall (blocking) manner.
        self.is_async = service.get(FLOWSERV_ASYNC)
        # Directory for temporary run files.
        basedir = service.get(FLOWSERV_BASEDIR)
        if basedir is None:
            raise err.MissingConfigurationError('API base directory')
        self.runsdir = service.get(FLOWSERV_RUNSDIR, os.path.join(basedir, DEFAULT_RUNSDIR))
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
                pool, _ = self.tasks[run_id]
                # Close the pool and terminate any running processes
                if pool is not None:
                    pool.close()
                    pool.terminate()
                # Delete the task from the dictionary. The state of the
                # respective run will be updated by the workflow engine that
                # uses this controller for workflow execution
                del self.tasks[run_id]

    def exec_workflow(self, run, template, arguments):
        """Initiate the execution of a given workflow template for a set of
        argument values. This will start a new process that executes a serial
        workflow asynchronously.

        The serial workflow engine executes workflows on the local machine and
        therefore uses the file system to store temporary run files. The path
        to the run folder is returned as the second value in the result tuple.
        The first value in the result tuple is the state of the workflow after
        the process is stated. If the workflow is executed asynchronously the
        state will be RUNNING. Otherwise, the run state should be an inactive
        state.

        The set of arguments is not further validated. It is assumed that the
        validation has been performed by the calling code (e.g., the run
        service manager).

        If the state of the run handle is not pending, an error is raised.

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
        flowserv.model.workflow.state.WorkflowState, string

        Raises
        ------
        flowserv.error.DuplicateRunError
        """
        # Get the run state. Ensure that the run is in pending state
        if not run.is_pending():
            raise RuntimeError("invalid run state '{}'".format(run.state))
        state = run.state()
        rundir = os.path.join(self.runsdir, run.run_id)
        # Expand template parameters. Get (i) list of files that need to be
        # copied, (ii) the expanded commands that represent the workflow steps,
        # and (iii) the list of output files.
        sourcedir = self.fs.workflow_staticdir(run.workflow.workflow_id)
        wf = SerialWorkflow(template, arguments, sourcedir)
        try:
            # Copy template files to the run folder.
            self.fs.copy_folder(key=sourcedir, dst=rundir)
            # Store any given file arguments in the run folder.
            for key, para in wf.template.parameters.items():
                if para.is_file() and key in arguments:
                    file = arguments[key]
                    file.source().store(os.path.join(rundir, file.target()))
            # Create top-level folder for all expected result files.
            outputs = wf.output_files()
            util.create_directories(basedir=rundir, files=outputs)
            # Get list of commands to execute.
            commands = wf.commands()
            # Start a new process to run the workflow. Make sure to catch all
            # exceptions to set the run state properly
            state = state.start()
            if self.is_async:
                # Raise an error if the service manager is not given.
                if self.service is None:
                    raise ValueError('service manager not given')
                # Run steps asynchronously in a separate process
                pool = Pool(processes=1)
                task_callback_function = partial(
                    callback_function,
                    lock=self.lock,
                    tasks=self.tasks,
                    service=self.service
                )
                with self.lock:
                    self.tasks[run.run_id] = (pool, state)
                pool.apply_async(
                    self.exec_func,
                    args=(
                        run.run_id,
                        rundir,
                        state,
                        wf.output_files(),
                        commands
                    ),
                    callback=task_callback_function
                )
                return state, rundir
            else:
                # Run steps synchronously and block the controller until done
                _, _, state_dict = self.exec_func(
                    run.run_id,
                    rundir,
                    state,
                    wf.output_files(),
                    commands
                )
                return serialize.deserialize_state(state_dict), rundir
        except Exception as ex:
            # Set the workflow runinto an ERROR state
            logging.error(ex)
            return state.error(messages=util.stacktrace(ex)), rundir


# -- Helper Methods -----------------------------------------------------------

def callback_function(result, lock, tasks, service):
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
        service: contextlib,contextmanager
            Context manager to create an instance of the service API.
    """
    run_id, rundir, state_dict = result
    logging.info('finished run {} with {}'.format(run_id, state_dict))
    with lock:
        if run_id in tasks:
            # Close the pool and remove the entry from the task index
            pool, _ = tasks[run_id]
            pool.close()
            del tasks[run_id]
    state = serialize.deserialize_state(state_dict)
    try:
        with service() as api:
            api.runs().update_run(run_id=run_id, state=state, rundir=rundir)
    except Exception as ex:
        logging.error(ex)
        logging.debug('\n'.join(util.stacktrace(ex)))


def run_workflow(run_id, rundir, state, output_files, steps):
    """Execute a list of workflow steps synchronously. This is the worker
    function for asynchronous workflow executions. Starts by copying input
    files and then executes the workflow synchronously.

    Returns a tuple containing the run identifier, the folder with the run
    files, and a serialization of the workflow state.

    Parameters
    ----------
    run_id: string
        Unique run identifier
    rundir: string
        Path to the working directory of the workflow run
    state: flowserv.model.workflow.state.WorkflowState
        Current workflow state (to access the timestamps)
    output_files: list(string)
        Relative path of output files that are generated by the workflow run
    steps: list(flowserv.model.template.step.Step)
        List of expanded workflow steps from a template workflow specification

    Returns
    -------
    (string, string, dict)
    """
    logging.info('start run {}'.format(run_id))
    try:
        # The serial controller ignores the command environments. We start by
        # creating a list of all command statements
        statements = list()
        for step in steps:
            statements.extend(step.commands)
        # Run workflow step-by-step
        for cmd in statements:
            logging.info('{}'.format(cmd))
            # Each command is expected to be a shell command that is executed
            # using the subprocess package. The subprocess.run() method is
            # preferred for capturing output to STDERR but it does not exist
            # in Python3.6.
            try:
                proc = subprocess.run(
                    cmd,
                    cwd=rundir,
                    shell=True,
                    capture_output=True
                )
                if proc.returncode != 0:
                    # Return error state. Include STDERR in result
                    messages = list()
                    messages.append(proc.stderr.decode('utf-8'))
                    result_state = state.error(messages=messages)
                    doc = serialize.serialize_state(result_state)
                    return run_id, rundir, doc
            except (AttributeError, TypeError):
                try:
                    subprocess.check_output(
                        cmd,
                        cwd=rundir,
                        shell=True,
                        stderr=subprocess.STDOUT
                    )
                except subprocess.CalledProcessError as ex:
                    logging.error(ex)
                    strace = util.stacktrace(ex)
                    logging.debug('\n'.join(strace))
                    result_state = state.error(messages=strace)
                    doc = serialize.serialize_state(result_state)
                    return run_id, rundir, doc
        # Create list of output files that were generated.
        files = list()
        for relative_path in output_files:
            if os.path.exists(os.path.join(rundir, relative_path)):
                files.append(relative_path)
        # Workflow executed successfully
        result_state = state.success(files=files)
    except Exception as ex:
        logging.error(ex)
        strace = util.stacktrace(ex)
        logging.debug('\n'.join(strace))
        result_state = state.error(messages=strace)
    logging.info('finished run {}: {}'.format(run_id, result_state.type_id))
    return run_id, rundir, serialize.serialize_state(result_state)
