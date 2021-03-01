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
from typing import Dict, List, Optional, Tuple

import logging
import os

from flowserv.config import FLOWSERV_ASYNC, FLOWSERV_BASEDIR, FLOWSERV_RUNSDIR, DEFAULT_RUNSDIR
from flowserv.controller.base import WorkflowController
from flowserv.controller.serial.engine.runner import exec_workflow
from flowserv.controller.worker.factory import WorkerFactory
from flowserv.controller.serial.workflow.result import RunResult
from flowserv.model.workflow.step import ContainerStep
from flowserv.model.base import RunObject
from flowserv.model.files.factory import FS
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.state import WorkflowState
from flowserv.service.api import APIFactory

import flowserv.controller.serial.workflow.parser as parser
import flowserv.error as err
import flowserv.model.workflow.state as serialize
import flowserv.util as util


class SerialWorkflowEngine(WorkflowController):
    """The workflow engine is used to execute workflow templates for a given
    set of arguments. Each workflow is executed as a serial workflow. The
    individual workflow steps can be executed in a separate process on request.
    """
    def __init__(
        self, service: APIFactory, worker_config: Optional[Dict] = None
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
        worker_config: dict, default=None
            Mapping of container image identifier to worker specifications that
            are used to create an instance of a :class:ContainerEngine worker.
        """
        self.fs = FS(env=service)
        self.service = service
        self.worker_config = worker_config if worker_config else service.worker_config()
        logging.info("workers {}".format(self.worker_config))
        # The is_async flag controlls the default setting for asynchronous
        # execution. If the flag is False all workflow steps will be executed
        # in a sequentiall (blocking) manner.
        self.is_async = service.get(FLOWSERV_ASYNC)
        # Directory for temporary run files.
        basedir = service.get(FLOWSERV_BASEDIR)
        if basedir is None:
            raise err.MissingConfigurationError('API base directory')
        logging.info('base dir {}'.format(basedir))
        self.runsdir = service.get(FLOWSERV_RUNSDIR, os.path.join(basedir, DEFAULT_RUNSDIR))
        # Dictionary of all running tasks
        self.tasks = dict()
        # Lock to manage asynchronous access to the task dictionary
        self.lock = Lock()

    def cancel_run(self, run_id: str):
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

    def exec_workflow(
        self, run: RunObject, template: WorkflowTemplate, arguments: Dict,
        config: Optional[Dict] = None
    ) -> Tuple[WorkflowState, str]:
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

        The optional configuration object can be used to override the worker
        configuration that was provided at object instantiation. Expects a
        dictionary with an element `workers` that contains a mapping of container
        identifier to a container worker configuration object.

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
        config: dict, default=None
            Optional object to overwrite the worker configuration settings.

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
        # Get the worker configuration.
        worker_config = self.worker_config if not config else config.get('workers')
        # Get the source directory for static workflow files.
        sourcedir = self.fs.workflow_staticdir(run.workflow.workflow_id)
        # Get the list of workflow steps and the generated output files.
        steps, run_args, outputs = parser.parse_template(template=template, arguments=arguments)
        try:
            # Copy template files to the run folder.
            self.fs.copy_folder(key=sourcedir, dst=rundir)
            # Store any given file arguments in the run folder.
            for key, para in template.parameters.items():
                if para.is_file() and key in arguments:
                    file = arguments[key]
                    file.source().store(os.path.join(rundir, file.target()))
            # Create top-level folder for all expected result files.
            util.create_directories(basedir=rundir, files=outputs)
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
                    run_workflow,
                    args=(
                        run.run_id,
                        rundir,
                        state,
                        outputs,
                        steps,
                        run_args,
                        WorkerFactory(config=worker_config)
                    ),
                    callback=task_callback_function
                )
                return state, rundir
            else:
                # Run steps synchronously and block the controller until done
                _, _, state_dict = run_workflow(
                    run_id=run.run_id,
                    rundir=rundir,
                    state=state,
                    output_files=outputs,
                    steps=steps,
                    arguments=run_args,
                    workers=WorkerFactory(config=worker_config)
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


def run_workflow(
    run_id: str, rundir: str, state: WorkflowState, output_files: List[str],
    steps: List[ContainerStep], arguments: Dict, workers: WorkerFactory
) -> Tuple[str, str, Dict]:
    """Execute a list of workflow steps synchronously.

    This is the worker function for asynchronous workflow executions. Returns a
    tuple containing the run identifier, the folder with the run files, and a
    serialization of the workflow state.

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
    steps: list of flowserv.model.workflow.step.WorkflowStep
        Steps in the serial workflow that are executed in the given context.
    arguments: dict
        Dictionary of argument values for parameters in the template.
    workers: flowserv.controller.worker.factory.WorkerFactory, default=None
        Factory for :class:ContainerStep steps.

    Returns
    -------
    (string, string, dict)
    """
    logging.info('start run {}'.format(run_id))
    try:
        run_result = exec_workflow(
            steps=steps,
            workers=workers,
            rundir=rundir,
            result=RunResult(arguments=arguments)
        )
        if run_result.returncode != 0:
            # Return error state. Include STDERR in result
            messages = run_result.log
            result_state = state.error(messages=messages)
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
