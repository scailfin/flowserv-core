# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory for workers that implement the :class:`flowserv.controller.worker.base.Worker`
class. Workers are used to initiate and control the excution of workflow steps
using different execution backends and implementations.

Instances of worker classes are created from a configuration specifications that
follow the following schema:

.. code-block:: yaml

    definitions:
      keyValuePair:
        description: Key-value pair object.
        properties:
          key:
            description: Value key.
            type: string
          value:
            anyOf:
            - type: integer
            - type: string
            description: Scalar value associated with the key.
        required:
        - key
        - value
        type: object
      workerSpec:
        description: Specification for a worker engine instance.
        properties:
          env:
            description: Key-value pairs for environment variables.
            items:
              $ref: '#/definitions/keyValuePair'
            type: array
          name:
            description: Unique worker identifier.
            type: string
          type:
            description: Worker type identifier
            enum:
            - code
            - docker
            - subprocess
            type: string
          vars:
            description: Key-value pairs for template string variables.
            items:
              $ref: '#/definitions/keyValuePair'
            type: array
          volume:
            description: Storage volume the worker has access to.
            type: string
        required:
        - name
        - type
        type: object
"""

from typing import Dict, List, Optional

from flowserv.controller.worker.base import Worker
from flowserv.controller.worker.code import CodeWorker, CODE_WORKER
from flowserv.controller.worker.config import java_jvm, python_interpreter
from flowserv.controller.worker.docker import DockerWorker, DOCKER_WORKER
from flowserv.controller.worker.docker import NotebookDockerWorker, NOTEBOOK_DOCKER_WORKER
from flowserv.controller.worker.notebook import NotebookEngine, NOTEBOOK_WORKER
from flowserv.controller.worker.subprocess import SubprocessWorker, SUBPROCESS_WORKER
from flowserv.model.workflow.step import WorkflowStep

import flowserv.error as err
import flowserv.util as util


"""Create an instance of the sub-process worker that is used as the default
worker for container steps that do not have a responsible worker defined for
them.
"""
default_container_worker = SubprocessWorker(
    variables={'python': python_interpreter(), 'java': java_jvm()}
)


"""Serialization label for worker identifier."""
WORKER_ID = 'name'


class WorkerPool(object):
    """Manager for a pool of worker instances. Workers are responsible for the
    initiation and control of the execution of steps in a serial workflow.

    Workers are instantiated from a dictionary serializations that follows the
    `workerSpec` schema defined in the `schema.json` file.
    """
    def __init__(
        self, workers: Optional[List[Dict]] = list(),
        managers: Optional[Dict] = None
    ):
        """Initialize the specifications for the workers that are managed by
        this worker pool and the optional list of task managers for individual
        workflow steps.

        Parameters
        ----------
        workers: list, default=list
            List of worker specifications.
        managers: dict, default=None
            Mapping from workflow step identifier to worker identifier that
            defines the worker that is responsible for the execution of the
            respective workflow step.
        """
        # Index of worker specifications.
        self._workerspecs = {doc['name']: doc for doc in workers}
        # Cache for created engine instance.
        self._workers = dict()
        self.managers = managers if managers is not None else dict()

    def get(self, step: WorkflowStep) -> Worker:
        """Get the instance of the worker that is associated with the given
        workflow step.

        If no worker specification exists for the given step a default worker
        is returned. The type of the default worker depends on the type of the
        workflow step. For code steps, currently only one type of worker
        exists. For container steps, a sub-process worker is used as the default
        worker.

        Parameters
        ----------
        step: flowserv.model.workflow.step.WorkflowStep
            Step in a serial workflow.

        Returns
        -------
        flowserv.controller.worker.base.Worker
        """
        # Return the worker that is associated with the given step via the
        # manager mapping (if defined).
        identifier = self.managers.get(step.name)
        if identifier is None:
            return self.get_default_worker(step)
        # Return the worker from the cache if it exists.
        if identifier in self._workers:
            return self._workers[identifier]
        # Get the worker specification for the container image. Raise an error
        # if the identifier is unknown.
        if identifier not in self._workerspecs:
            raise err.UnknownObjectError(obj_id=identifier, type_name='worker')
        # Create the worker and add it to the cache before returning it.
        worker = create_worker(self._workerspecs[identifier])
        self._workers[identifier] = worker
        return worker

    def get_default_worker(self, step: WorkflowStep) -> Worker:
        """Return the default worker depending on the type of the given
        workflow step.

        Parameters
        ----------
        step: flowserv.model.workflow.step.WorkflowStep
            Step in a serial workflow.

        Returns
        -------
        flowserv.controller.worker.base.Worker
        """
        if step.is_code_step():
            return CodeWorker()
        elif step.is_container_step():
            return default_container_worker
        elif step.is_notebook_step():
            return NotebookEngine()
        raise ValueError(f"unknown step type '{step.step_type}'")


# -- Helper Functions for Worker configurations -------------------------------

def create_worker(doc: Dict) -> Worker:
    """Factory pattern for workers.

    Create an instance of a worker implementation from a given worker
    serialization.

    Parameters
    ----------
    doc: dict
        Dictionary serialization for a worker.

    Returns
    -------
    flowserv.controller.worker.base.Worker
    """
    identifier = doc['name']
    worker_type = doc['type']
    env = util.to_dict(doc.get('env', []))
    vars = util.to_dict(doc.get('variables', []))
    volume = doc.get('volume')
    if worker_type == SUBPROCESS_WORKER:
        return SubprocessWorker(
            variables=vars,
            env=env,
            identifier=identifier,
            volume=volume
        )
    elif worker_type == DOCKER_WORKER:
        return DockerWorker(
            variables=vars,
            env=env,
            identifier=identifier,
            volume=volume
        )
    elif worker_type == CODE_WORKER:
        return CodeWorker(identifier=identifier, volume=volume)
    elif worker_type == NOTEBOOK_WORKER:
        return NotebookEngine(identifier=identifier, volume=volume)
    elif worker_type == NOTEBOOK_DOCKER_WORKER:
        return NotebookDockerWorker(identifier=identifier, env=env, volume=volume)
    raise ValueError(f"unknown worker type '{worker_type}'")


def WorkerSpec(
    worker_type: str, identifier: Optional[str] = None, variables: Optional[Dict] = None,
    env: Optional[Dict] = None, volume: Optional[str] = None
) -> Dict:
    """Get a serialization for a worker specification.

    Parameters
    ----------
    worker_type: string
        Unique worker type identifier.
    identifier: string, default=None
        Unique worker identifier. If no identifier is given, a new unique
        identifier will be generated.
    variables: dict, default=None
        Mapping with default values for placeholders in command template
        strings.
    env: dict, default=None
        Default settings for environment variables when executing workflow
        steps. These settings can get overridden by step-specific settings.
    volume: string, default=None
        Identifier for the storage volume that the worker has access to.

    Returns
    -------
    dict
    """
    # Set optional environment and variables dictionaries if not given.
    env = env if env is not None else dict()
    variables = variables if variables is not None else dict()
    doc = {
        WORKER_ID: identifier if identifier is not None else util.get_unique_identifier(),
        'type': worker_type,
        'env': [util.to_kvp(key=k, value=v) for k, v in env.items()],
        'variables': [util.to_kvp(key=k, value=v) for k, v in variables.items()]
    }
    if volume:
        doc['volume'] = volume
    return doc


def Code(identifier: Optional[str] = None, volume: Optional[str] = None) -> Dict:
    """Get base configuration serialization for a code worker.

    Parameters
    ----------
    identifier: string, default=None
        Unique worker identifier. If no identifier is given, a new unique
        identifier will be generated.
    volume: string, default=None
        Identifier for the storage volume that the worker has access to.

    Returns
    -------
    dict
    """
    return WorkerSpec(
        worker_type=CODE_WORKER,
        identifier=identifier,
        volume=volume
    )


def Docker(
    identifier: Optional[str] = None, variables: Optional[Dict] = None,
    env: Optional[Dict] = None, volume: Optional[str] = None
) -> Dict:
    """Get base configuration for a subprocess worker with the given optional
    arguments.

    Parameters
    ----------
    identifier: string, default=None
        Unique worker identifier. If no identifier is given, a new unique
        identifier will be generated.
    variables: dict, default=None
        Mapping with default values for placeholders in command template
        strings.
    env: dict, default=None
        Default settings for environment variables when executing workflow
        steps. These settings can get overridden by step-specific settings.
    volume: string, default=None
        Identifier for the storage volume that the worker has access to.

    Returns
    -------
    dict
    """
    return WorkerSpec(
        worker_type=DOCKER_WORKER,
        identifier=identifier,
        variables=variables,
        env=env,
        volume=volume
    )


def Notebook(identifier: Optional[str] = None, volume: Optional[str] = None) -> Dict:
    """Get base configuration serialization for a notebook worker.

    Parameters
    ----------
    identifier: string, default=None
        Unique worker identifier. If no identifier is given, a new unique
        identifier will be generated.
    volume: string, default=None
        Identifier for the storage volume that the worker has access to.

    Returns
    -------
    dict
    """
    return WorkerSpec(
        worker_type=NOTEBOOK_WORKER,
        identifier=identifier,
        volume=volume
    )


def Subprocess(
    identifier: Optional[str] = None, variables: Optional[Dict] = None,
    env: Optional[Dict] = None, volume: Optional[str] = None
) -> Dict:
    """Get base configuration for a subprocess worker with the given optional
    arguments.

    Parameters
    ----------
    identifier: string, default=None
        Unique worker identifier. If no identifier is given, a new unique
        identifier will be generated.
    variables: dict, default=None
        Mapping with default values for placeholders in command template
        strings.
    env: dict, default=None
        Default settings for environment variables when executing workflow
        steps. These settings can get overridden by step-specific settings.
    volume: string, default=None
        Identifier for the storage volume that the worker has access to.

    Returns
    -------
    dict
    """
    return WorkerSpec(
        worker_type=SUBPROCESS_WORKER,
        variables=variables,
        env=env,
        identifier=identifier,
        volume=volume
    )
