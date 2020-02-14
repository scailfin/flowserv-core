# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation for a workflow controller backend that is capable of running
serial workflow specification. This controller allows execution in workflow
steps within separate sub-processes.
"""

from __future__ import print_function

import logging
import os
import subprocess

from functools import partial
from multiprocessing import Lock, Pool

from flowserv.controller.base import WorkflowController
from flowserv.controller.serial.workflow import SerialWorkflow
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.resource import FSObject

import flowserv.core.util as util
import flowserv.model.template.parameter as tp
import flowserv.model.workflow.state as serialize


"""Additional environment variables that control the configuration of the
serial workflow controller.
"""
SERIAL_ENGINE_ASYNC = 'SERIAL_ENGINE_ASYNC'


class SerialWorkflowEngine(WorkflowController):
    """The workflow engine is used to execute workflow templates for a given
    set of arguments. Each workflow is executed as a serial workflow. The
    individual workflow steps can be executed in a separate process on request.
    """
    def __init__(self, exec_func=None, is_async=None, verbose=False):
        """Initialize the function that is used to execute individual workflow
        steps. The run workflow function in this module executes all steps
        within sub-processes in the same environment as the workflow
        controller.

        NOTE: Using the provided execution function is intended for development
        and private use only. It is not recommended (and very dangerous) to
        use this function in a public setting.

        The is_async flag controlls the default setting for asynchronous
        execution. If the flag is False all workflow steps will be executed
        in a sequentiall (blocking) manner unless overridden by the in_sync
        flag by the execute method.

        Parameters
        ----------
        run_workflow: func, optional
            Function that is used to execute the workflow commands
        is_async: bool, optional
            Flag that determines whether workflows execution is synchronous or
            asynchronous by default.
        verbose: bool, optional
            Print command strings to STDOUT during workflow execution
        """
        self.exec_func = exec_func if exec_func is not None else run_workflow
        # Set the is_async flag. If no value is given the default value is set
        # from the respective environment variable
        if is_async is not None:
            self.is_async = is_async
        else:
            self.is_async = bool(os.environ.get(SERIAL_ENGINE_ASYNC, 'True'))
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
                if pool is not None:
                    pool.close()
                    pool.terminate()
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
        return [(SERIAL_ENGINE_ASYNC, str(self.is_async))]

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
        state = run.state
        # Expand template parameters. Get (i) list of files that need to be
        # copied, (ii) the expanded commands that represent the workflow steps,
        # and (iii) the list of output files.
        wf = SerialWorkflow(template, arguments)
        try:
            # Copy all necessary files to the run folder
            util.copy_files(files=wf.upload_files(), target_dir=run.rundir)
            # Create top-level folder for all expected result files (if it does not
            # exist already)
            output_files = wf.output_files()
            for filename in output_files:
                dirname = os.path.dirname(filename)
                if dirname:
                    # Create the directory if it does not exist
                    out_dir = os.path.join(run.rundir, dirname)
                    if not os.path.isdir(out_dir):
                        os.makedirs(out_dir)
            # Get list of commands to execute.
            commands = wf.commands()
            # Start a new process to run the workflow. Make sure to catch all
            # exceptions to set the run state properly
            state = state.start()
            if RUN_ASYNC(run_async=run_async, is_async=self.is_async):
                # Run steps asynchronously in a separate process
                pool = Pool(processes=1)
                task_callback_function = partial(
                    callback_function,
                    lock=self.lock,
                    tasks=self.tasks
                )
                with self.lock:
                    self.tasks[run.identifier] = pool
                pool.apply_async(
                    self.exec_func,
                    args=(
                        run.identifier,
                        run.rundir,
                        state,
                        output_files,
                        commands,
                        self.verbose
                    ),
                    callback=task_callback_function
                )
                return state
            else:
                # Run steps synchronously and block the controller until done
                _, state_dict = self.exec_func(
                    run.identifier,
                    run.rundir,
                    state,
                    output_files,
                    commands,
                    self.verbose
                )
                return serialize.deserialize_state(state_dict)
        except Exception as ex:
            # Set the workflow runinto an ERROR state
            logging.error(ex)
            return state.error(messages=util.stacktrace(ex))

    def modify_template(self, template, parameters):
        """Modify a the workflow specification in a given template by adding
        the a set of parameters. If a parameter in the added parameters set
        already exists in the template the name, index, default value, the
        value list and the required flag of the existing parameter are replaced
        by the values of the given parameter.

        Returns a modified workflow template. Raises an error if the parameter
        identifier in the resulting template are no longer unique.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template handle.
        parameters: dict(flowserv.model.parameter.base.TemplateParameter)
            Additional template parameters

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate

        Raises
        ------
        flowserv.core.error.InvalidTemplateError
        """
        # Get a copy of the files and parameters sections of the inputs
        # declaration
        workflow_spec = template.workflow_spec
        inputs = workflow_spec.get('inputs', dict())
        in_files = list(inputs.get('files', list()))
        in_params = dict(inputs.get('parameters', dict()))
        # Ensure that the identifier for all parameters are unique
        para_merge = dict(template.parameters)
        for para in parameters.values():
            if para.identifier in para_merge:
                para = para_merge[para.identifier].merge(para)
            para_merge[para.identifier] = para
            # Depending on whether the type of the parameter is a file or not we
            # add a parameter reference to the respective input section
            if para.is_file():
                in_files.append(tp.VARIABLE(para.identifier))
            else:
                if para.identifier not in in_params:
                    in_params[para.identifier] = tp.VARIABLE(para.identifier)
        spec = dict(workflow_spec)
        spec['inputs'] = {'files': in_files, 'parameters': in_params}
        return WorkflowTemplate(
            workflow_spec=spec,
            sourcedir=template.sourcedir,
            parameters=para_merge,
            modules=template.modules,
            postproc_spec=template.postproc_spec,
            result_schema=template.result_schema
        )


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
            pool = tasks[run_id]
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


def run_workflow(run_id, rundir, state, output_files, steps, verbose):
    """Execute a list of workflow steps synchronously. This is the worker
    function for asynchronous workflow executions. Starts by copying input
    files and then executes the workflow synchronously.

    Returns a tuple containing the task identifier and a serialization of the
    workflow state.

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
    verbose: bool, optional
        Output executed command statements if flag is True

    Returns
    -------
    (string, dict)
    """
    logging.debug('start run {}'.format(run_id))
    try:
        # The serial controller ignores the command environments. We start by
        # creating a list of all command statements
        statements = list()
        for step in steps:
            statements.extend(step.commands)
        # Run workflow step-by-step
        for cmd in statements:
            # Print command if verbose
            if verbose:
                print('{}'.format(cmd))
            # Each command is expected to be a shell command that is executed
            # using the subprocess package. The subprocess.run() method is
            # preferred for capturing output to STDERR but it does not exist
            # in Python2.
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
                    return run_id, serialize.serialize_state(result_state)
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
                    result_state = state.error(messages=util.stacktrace(ex))
                    return run_id, serialize.serialize_state(result_state)
        # Create dictionary of output files
        files = list()
        for resource_name in output_files:
            f = FSObject(
                identifier=util.get_unique_identifier(),
                name=resource_name,
                filename=os.path.join(rundir, resource_name)
            )
            files.append(f)
        # Workflow executed successfully
        result_state = state.success(resources=files)
    except Exception as ex:
        logging.error(ex)
        result_state = state.error(messages=util.stacktrace(ex))
    logging.debug('finished run {} = {}'.format(run_id, result_state.type_id))
    return run_id, serialize.serialize_state(result_state)


def RUN_ASYNC(run_async, is_async):
    """Determine whether to run a workflow synchronously or asynchronously
    based on the value of the run_async flag and the default is_async value.
    Either value may be None.

    By default, workflows are run asynchronously.

    Parameters
    ----------
    run_async: bool
        Run async flag provided to the execute method
    is_async: bool
        Default value if the run_async flag is None

    Returns
    -------
    bool
    """
    # If the run_async parameter was provided the value determines the running
    # mode.
    if run_async is not None:
        return run_async
    # Use the provided default value (if given)
    if is_async is not None:
        return is_async
    # By default all workflows are being run asynchronously
    return True
