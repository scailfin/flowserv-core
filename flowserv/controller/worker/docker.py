# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of a workflow step engine that uses the local Docker daemon
to execute workflow steps.
"""

from typing import Dict, Optional

import logging
import os

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.model.workflow.step import ContainerStep
from flowserv.controller.worker.base import ContainerEngine

import flowserv.util as util


class DockerWorker(ContainerEngine):
    """Container step engine that uses the local Docker deamon to execute the
    commands in a workflow step.
    """
    def __init__(self, variables: Optional[Dict] = None, env: Optional[Dict] = None):
        """Initialize the optional mapping with default values for placeholders
        in command template strings.

        Parameters
        ----------
        variables: dict, default=None
            Mapping with default values for placeholders in command template
            strings.
        env: dict, default=None
            Default settings for environment variables when executing workflow
            steps. These settings can get overridden by step-specific settings.
        """
        super(DockerWorker, self).__init__(variables=variables, env=env)

    def run(self, step: ContainerStep, env: Dict, rundir: str) -> ExecResult:
        """Execute a list of commands from a workflow steps synchronously using
        the Docker engine.

        Stops execution if one of the commands fails. Returns the combined
        result from all the commands that were executed.

        Parameters
        ----------
        step: flowserv.controller.serial.workflow.ContainerStep
            Step in a serial workflow.
        env: dict, default=None
            Default settings for environment variables when executing workflow
            steps. May be None.
        rundir: string
            Path to the working directory of the workflow run that this step
            belongs to.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        logging.info('run step with Docker worker')
        # Keep output to STDOUT and STDERR for all executed commands in the
        # respective attributes of the returned execution result.
        result = ExecResult(step=step)
        # Setup the workflow environment by obtaining volume information for
        # all directories in the run folder.
        volumes = dict()
        for filename in os.listdir(rundir):
            abs_file = os.path.abspath(os.path.join(rundir, filename))
            if os.path.isdir(abs_file):
                volumes[abs_file] = {'bind': '/{}'.format(filename), 'mode': 'rw'}
        # Run the individual commands using the local Docker deamon. Import
        # docker package here to avoid errors for installations that do not
        # intend to use Docker and therefore did not install the package.
        import docker
        from docker.errors import ContainerError, ImageNotFound, APIError
        client = docker.from_env()
        try:
            for cmd in step.commands:
                logging.info('{}'.format(cmd))
                logs = client.containers.run(
                    image=step.image,
                    command=cmd,
                    volumes=volumes,
                    remove=True,
                    environment=env,
                    stdout=True
                )
                if logs:
                    result.stdout.append(logs.decode('utf-8'))
        except (ContainerError, ImageNotFound, APIError) as ex:
            logging.error(ex)
            strace = '\n'.join(util.stacktrace(ex))
            logging.debug(strace)
            result.stderr.append(strace)
            result.exception = ex
            result.returncode = 1
        return result
