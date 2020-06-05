# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of a workflow controller for serial workflows that uses the
local Docker daemon to execute workflow steps.
"""

import docker
import logging
import os

from docker.errors import ContainerError, ImageNotFound, APIError

from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.model.workflow.resource import FSObject

import flowserv.core.util as util
import flowserv.model.workflow.state as serialize


class DockerWorkflowEngine(SerialWorkflowEngine):
    """The docker workflow engine is used to execute workflow templates for a
    given set of arguments using docker containers.

    the engine extends the multi-process controller for asynchronous execution.
    Workflow runs are executed by the docker_run() function.
    """
    def __init__(self, is_async=None):
        """Initialize the super class using the docker_run execution function.

        Parameters
        ----------
        is_async: bool, optional
            Flag that determines whether workflows execution is synchronous or
            asynchronous by default.
        """
        super(DockerWorkflowEngine, self).__init__(
            exec_func=docker_run,
            is_async=is_async,
            verbose=True
        )


# -- Workflow execution function ----------------------------------------------


def docker_run(run_id, rundir, state, output_files, steps, verbose):
    """Execute a list of workflow steps synchronously using the Docker engine.

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
    print('start docker run {}'.format(run_id))
    logging.debug('start docker run {}'.format(run_id))
    # Setup the workflow environment by obtaining volume information for all
    # directories in the run folder.
    volumes = dict()
    for filename in os.listdir(rundir):
        abs_file = os.path.abspath(os.path.join(rundir, filename))
        if os.path.isdir(abs_file):
            volumes[abs_file] = {'bind': '/{}'.format(filename), 'mode': 'rw'}
    # Run the individual workflow steps using the local Docker deamon.
    client = docker.from_env()
    try:
        for step in steps:
            for cmd in step.commands:
                if verbose:
                    print('{}'.format(cmd))
                client.containers.run(
                    image=step.env,
                    command=cmd,
                    volumes=volumes
                )
    except (ContainerError, ImageNotFound, APIError) as ex:
        logging.error(ex)
        result_state = state.error(messages=[str(ex)])
        return run_id, serialize.serialize_state(result_state)
    # Create dictionary of output files
    files = list()
    for resource_name in output_files:
        try:
            f = FSObject(
                identifier=util.get_unique_identifier(),
                name=resource_name,
                filename=os.path.join(rundir, resource_name)
            )
        except (OSError, IOError) as ex:
            logging.error(ex)
            result_state = state.error(messages=[str(ex)])
            return run_id, serialize.serialize_state(result_state)
        files.append(f)
    # Workflow executed successfully
    result_state = state.success(resources=files)
    logging.debug('finished run {} = {}'.format(run_id, result_state.type_id))
    return run_id, serialize.serialize_state(result_state)
