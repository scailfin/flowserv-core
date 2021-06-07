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

from typing import Dict, List, Optional, Tuple

import logging
import os
import tempfile
import shutil

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.model.workflow.step import ContainerStep, NotebookStep
from flowserv.controller.worker.base import ContainerWorker, Worker
from flowserv.volume.fs import FileSystemStorage

import flowserv.util as util


"""Unique type identifier for DockerWorker serializations."""
DOCKER_WORKER = 'docker'


class DockerWorker(ContainerWorker):
    """Container step engine that uses the local Docker deamon to execute the
    commands in a workflow step.
    """
    def __init__(
        self, variables: Optional[Dict] = None, env: Optional[Dict] = None,
        identifier: Optional[str] = None, volume: Optional[str] = None
    ):
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
        identifier: string, default=None
            Unique worker identifier. If the value is None a new unique identifier
            will be generated.
        volume: string, default=None
            Identifier for the storage volume that the worker has access to.
            By default, the worker is expected to have access to the default
            volume store for a workflow run.
        """
        super(DockerWorker, self).__init__(
            variables=variables,
            env=env,
            identifier=identifier,
            volume=volume
        )

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
        return docker_run(
            image=step.image,
            commands=step.commands,
            env=env,
            rundir=rundir,
            result=ExecResult(step=step)
        )


"""Unique type identifier for NotebookDockerWorker serializations."""
NOTEBOOK_DOCKER_WORKER = 'nbdocker'


"""Default Dockerfile for created papermill containers."""
DOCKERFILE = [
    'FROM python:3.8',
    'COPY requirements.txt /app/requirements.txt',
    'WORKDIR /app',
    'RUN pip install jupyter',
    'RUN pip install papermill',
    'RUN pip install -r requirements.txt',
    'RUN rm -Rf /app',
    'WORKDIR /'
]


class NotebookDockerWorker(Worker):
    """Execution engine for notebook steps in a serial workflow."""
    def __init__(
        self, env: Optional[Dict] = None, identifier: Optional[str] = None,
        volume: Optional[str] = None
    ):
        """Initialize the worker identifier and accessible storage volume.

        Parameters
        ----------
        env: dict, default=None
            Default settings for environment variables when executing workflow
            steps. These settings can get overridden by step-specific settings.
        identifier: string, default=None
            Unique worker identifier. If the value is None a new unique identifier
            will be generated.
        volume: string, default=None
            Identifier for the storage volume that the worker has access to.
            By default, the worker is expected to have access to the default
            volume store for a workflow run.
        """
        super(NotebookDockerWorker, self).__init__(identifier=identifier, volume=volume)
        self.env = env

    def exec(self, step: NotebookStep, context: Dict, store: FileSystemStorage) -> ExecResult:
        """Execute a given notebook workflow step in the current workflow
        context.

        The notebook engine expects a file system storage volume that provides
        access to the notebook file and any other aditional input files.

        Parameters
        ----------
        step: flowserv.model.workflow.step.NotebookStep
            Notebook step in a serial workflow.
        context: dict
            Dictionary of variables that represent the current workflow state.
        store: flowserv.volume.fs.FileSystemStorage
            Storage volume that contains the workflow run files.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        result = ExecResult(step=step)
        # Create Docker image including papermill and notebook requirements.
        try:
            image, logs = docker_build(name=step.name, requirements=step.requirements)
            if logs:
                result.stdout.append('\n'.join(logs))
        except Exception as ex:
            logging.error(ex, exc_info=True)
            strace = '\n'.join(util.stacktrace(ex))
            logging.debug(strace)
            result.stderr.append(strace)
            result.exception = ex
            result.returncode = 1
            return result
        # Run notebook in Docker container.
        cmd = step.cli_command(context=context)
        result.stdout.append(f'run: {cmd}')
        return docker_run(
            image=image,
            commands=[cmd],
            env=self.env,
            rundir=store.basedir,
            result=result
        )


# -- Helper Methods -----------------------------------------------------------

def docker_build(name: str, requirements: List[str]) -> Tuple[str, List[str]]:
    """Build a Docker image from a standard Python image with ``papermill`` and
    the given requirements installed.

    Returns the identifier of the created image.

    Parameters
    ----------
    name: string
        Name for the created image (derived from the workflow step name).
    requirements: list of string
        List of requirements that will be written to a file ``requirements.txt``
        and installed inside the created Docker image.

    Returns
    -------
    string, list of string
    """
    # Create a temporary folder for the Dockerfile.
    tmpdir = tempfile.mkdtemp()
    # Write requirements.txt to file.
    with open(os.path.join(tmpdir, 'requirements.txt'), 'wt') as f:
        for line in requirements:
            f.write(f'{line}\n')
    # Write Dockerfile.
    # TODO. In the future we may want to allow an option to read this from
    # a file that is specified via an environment variable.
    with open(os.path.join(tmpdir, 'Dockerfile'), 'wt') as f:
        for line in DOCKERFILE:
            f.write(f'{line}\n')
    # Build Docker image. Import docker package here to avoid errors for
    # installations that do not intend to use Docker and therefore did not
    # install the package.
    import docker
    client = docker.from_env()
    image, logs = client.images.build(path=tmpdir, tag=name, nocache=False)
    outputs = [doc['stream'] for doc in logs if doc.get('stream', '').strip()]
    client.close()
    # Remove temporary folder before returning the image identifier.
    shutil.rmtree(tmpdir)
    return image.tags[-1], outputs


def docker_run(
    image: str, commands: List[str], env: Dict, rundir: str, result: ExecResult
) -> ExecResult:
    """Helper function that executes a list of commands inside a Docker container.

    Parameters
    ----------
    image: string
        Identifier of the Docker image to run.
    commands: string or list of string
        Commands that are executed inside the Docker container.
    result: flowserv.controller.serial.workflow.result.ExecResult
        Result object that will contain the run outputs and status code.

    Returns
    -------
    flowserv.controller.serial.workflow.result.ExecResult
    """
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
        for cmd in commands:
            logging.info('{}'.format(cmd))
            # Run detached container to be able to capture output to
            # both, STDOUT and STDERR. DO NOT remove the container yet
            # in order to be able to get the captured outputs.
            container = client.containers.run(
                image=image,
                command=cmd,
                volumes=volumes,
                remove=False,
                environment=env,
                detach=True
            )
            # Wait for container to finish. The returned dictionary will
            # contain the container's exit code ('StatusCode').
            r = container.wait()
            # Add container logs to the standard outputs for the workflow
            # results.
            logs = container.logs()
            if logs:
                result.stdout.append(logs.decode('utf-8'))
            # Remove container if the remove flag is set to True.
            container.remove()
            # Check exit code for the container. If the code is not zero
            # an error occurred and we exit the commands loop.
            status_code = r.get('StatusCode')
            if status_code != 0:
                result.returncode = status_code
                break
    except (ContainerError, ImageNotFound, APIError) as ex:
        logging.error(ex, exc_info=True)
        strace = '\n'.join(util.stacktrace(ex))
        logging.debug(strace)
        result.stderr.append(strace)
        result.exception = ex
        result.returncode = 1
    client.close()
    return result
