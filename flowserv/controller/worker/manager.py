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
            - docker
            - subprocess
            type: string
          vars:
            description: Key-value pairs for template string variables.
            items:
              $ref: '#/definitions/keyValuePair'
            type: array
          volumes:
            description: List of storage volumns the worker has access to.
            items:
              type: string
            type: array
        required:
        - name
        - type
        type: object
"""

from typing import Dict, List, Optional

from flowserv.controller.worker.base import Worker
from flowserv.controller.worker.config import java_jvm, python_interpreter
from flowserv.controller.worker.docker import DockerWorker, DOCKER_WORKER
from flowserv.controller.worker.subprocess import SubprocessWorker, SUBPROCESS_WORKER

import flowserv.error as err
import flowserv.util as util


"""Create an instance of the default worker. By default a subprocess worker is
used to execute workflow steps.
"""
default_engine = SubprocessWorker(variables={'python': python_interpreter(), 'java': java_jvm()})


class WorkerPool(object):
    """Manager for a pool of worker instances. Workers are responsible for the
    initiation and control of the execution of steps in a serial workflow.

    Workers are instantiated from a dictionary serializations that follows the
    `workerSpec` schema defined in the `schema.json` file.
    """
    def __init__(self, workers: List[Dict]):
        """Initialize the specifications for the workers that are managed by
        this worker pool.

        Parameters
        ----------
        workers: list
            List of worker specifications.
        """
        # Index of worker specifications.
        self._workerspecs = {doc['id']: doc for doc in workers}
        # Cache for created engine instance.
        self._workers = dict()

    def get(self, identifier: str) -> Worker:
        """Get the instance of the worker that is associated with the given
        identifier.

        If no worker specification exists for the given identifier a unknown
        object error is raised.

        Parameters
        ----------
        image: string
            Container image identifier.

        Returns
        -------
        flowserv.controller.worker.base.Worker
        """
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
    identifier = doc['id']
    worker_type = doc['type']
    env = util.to_dict(doc.get('env', []))
    vars = util.to_dict(doc.get('vars', []))
    if worker_type == SUBPROCESS_WORKER:
        return SubprocessWorker(variables=vars, env=env, identifier=identifier)
    elif worker_type == DOCKER_WORKER:
        return DockerWorker(variables=vars, env=env, identifier=identifier)
    raise ValueError(f"unknown worker type '{worker_type}'")


def WorkerSpec(
    worker_type: str, identifier: str, variables: Optional[Dict] = None,
    env: Optional[Dict] = None,
) -> Dict:
    """Get a serialization for a worker specification.

    Parameters
    ----------
    worker_type: string
        Uniuqe worker type identifier.
    identifier: string
        Uniuqe worker identifier.
    variables: dict, default=None
        Mapping with default values for placeholders in command template
        strings.
    env: dict, default=None
        Default settings for environment variables when executing workflow
        steps. These settings can get overridden by step-specific settings.

    Returns
    -------
    dict
    """
    # Set optional environment and variables dictionaries if not given.
    env = env if env is not None else dict()
    variables = variables if variables is not None else dict()
    return {
        'id': identifier,
        'type': worker_type,
        'env': [util.to_kvp(key=k, value=v) for k, v in env.items()],
        'vars': [util.to_kvp(key=k, value=v) for k, v in variables.items()]
    }


def Docker(identifier: str, variables: Optional[Dict] = None, env: Optional[Dict] = None,) -> Dict:
    """Get base configuration for a subprocess worker with the given optional
    arguments.

    Parameters
    ----------
    identifier: string
        Uniuqe worker identifier.
    variables: dict, default=None
        Mapping with default values for placeholders in command template
        strings.
    env: dict, default=None
        Default settings for environment variables when executing workflow
        steps. These settings can get overridden by step-specific settings.

    Returns
    -------
    dict
    """
    return WorkerSpec(
        worker_type=DOCKER_WORKER,
        identifier=identifier,
        variables=variables,
        env=env
    )


def Subprocess(identifier: str, variables: Optional[Dict] = None, env: Optional[Dict] = None,) -> Dict:
    """Get base configuration for a subprocess worker with the given optional
    arguments.

    Parameters
    ----------
    identifier: string
        Uniuqe worker identifier.
    variables: dict, default=None
        Mapping with default values for placeholders in command template
        strings.
    env: dict, default=None
        Default settings for environment variables when executing workflow
        steps. These settings can get overridden by step-specific settings.

    Returns
    -------
    dict
    """
    return WorkerSpec(
        worker_type=SUBPROCESS_WORKER,
        variables=variables,
        env=env,
        identifier=identifier
    )
