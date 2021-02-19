# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory for workers that implement the :class:ContainerEngine class. Workers
are used to excute workflow steps using different, user-defined configurations.

Instances of worker classes are created from a configuration specifications that
follow the following schema:

  workerRef:
    description: Reference the class that extends the abstract container engine.
    oneOf:
    - description: Worker class reference.
      properties:
        className:
          description: Name of the class that implements the worker.
          type: string
        moduleName:
          description: Module that contains the class that implements the worker.
          type: string
        required:
        - className
        - moduleName
      type: object
    - description: Unique worker identifier.
      enum:
      - docker
      - subprocess
      type: string
  workerSpec:
    description: Specification for a worker engine that is associated with a container
      image.
    properties:
      args:
        type: object
      image:
        description: Conatiner image identifier.
        type: string
      worker:
        $ref: '#/definitions/workerRef'
    required:
    - image
    - worker
    type: object
"""

from __future__ import annotations
from importlib import import_module
from jsonschema import Draft7Validator, RefResolver
from typing import Dict, List, Optional

import importlib.resources as pkg_resources
import json
import os
import yaml

from flowserv.controller.serial.worker.base import ContainerEngine
from flowserv.controller.serial.worker.config import java_jvm, python_interpreter
from flowserv.controller.serial.worker.docker import DockerWorker
from flowserv.controller.serial.worker.subprocess import SubprocessWorker


"""Create an instance of the default worker. By default a subprocess worker is
used to execute workflow steps.
"""
default_engine = SubprocessWorker(variables={'python': python_interpreter(), 'java': java_jvm()})


"""Create schema validator for API requests."""
schemafile = os.path.abspath(os.path.join(__file__, 'schema.json'))
schema = json.load(pkg_resources.open_text(__package__, 'schema.json'))
resolver = RefResolver(schemafile, schema)
validator = Draft7Validator(schema=schema['definitions']['workerSpec'], resolver=resolver)


class WorkerFactory(object):
    """Factory for workers that implement the :class:ContainerEngine class.

    Workers are instantiated from a dictionary that follows the `workerSpec`
    schema defined in the `schema.json` file.
    """
    def __init__(self, config: Optional[Dict] = None, validate: Optional[bool] = False):
        """Initialize the dictionary that contains the mapping of container
        image identifier to worker specification.

        If the validate flag is True the given worker specifications are
        validated against the `workerSpec` schema.

        Parameters
        ----------
        config: dict, default=None
            Mapping of container image identifier to worker specifications that
            are used to create an instance of a :class:ContainerEngine worker.
        validate: bool, default=False
            Validate the given worker specifications against the `workerSpec`
            schema if True.
        """
        self.config = config if config is not None else dict()
        if validate:
            for spec in self.config.values():
                validator.validate(spec)
        # Replace callables in the worker arguments with their evaluation result.
        for spec in self.config.values():
            args = spec.get('args', dict())
            for key in args:
                if callable(args[key]):
                    f = args[key]
                    args[key] = f()
        # Cache for created engine instance.
        self._workers = dict()

    def get(self, image: str) -> ContainerEngine:
        """Get the instance of the container engine that is associated with the
        given container image identifier.

        If no worker specification exists for the given identifier in the factory
        configuration the default engine is returned.

        Parameters
        ----------
        image: string
            Container image identifier.

        Returns
        -------
        flowserv.controller.serial.worker.base.ContainerEngine
        """
        # Return the worker from the cache if it exists.
        if image in self._workers:
            return self._workers[image]
        # Get the worker specification for the container image.
        spec = self.config.get(image)
        if spec is None:
            # Return the default engine if no entry exist in the internal
            # configuration for the given container image.
            return default_engine
        # Get the class object and the initail arguments for the worker from
        # the specification.
        worker_spec = spec['worker']
        worker_args = spec.get('args', dict())
        if isinstance(worker_spec, dict):
            # Load class from 'moduleName' and 'className' in the worker
            # specification.
            module = import_module(worker_spec['moduleName'])
            worker_cls = getattr(module, worker_spec['className'])
        elif worker_spec == 'docker':
            worker_cls = DockerWorker
        else:  # worker_spec == 'subprocess'
            worker_cls = SubprocessWorker
        # Create the worker and add it to the cache before returning it.
        worker = worker_cls(**worker_args)
        self._workers[image] = worker
        return worker

    @staticmethod
    def load(doc: List[Dict], validate: Optional[bool] = False) -> WorkerFactory:
        """Create an instance of the worker factory from a list of worker
        specifications.

        Convertes the list of specifications into a dictionary where the
        specifications are mapped to their image identifier. Validates the
        documents against the `workerConfig` schema if the `validate` flag is
        True.

        Parameters
        ----------
        doc: list of dict
            List of dictionaries that follow the `workerSpec` schema.
        validate: bool, default=False
            Validate the given worker specifications against the `workerSpec`
            schema if True.

        Returns
        -------
        flowserv.controller.serial.worker.factory.WorkerFactory
        """
        # Convert list of specifications to a dictionary.
        workers = dict()
        for spec in doc:
            if validate:
                validator.validate(spec)
            workers[spec['image']] = spec
        return WorkerFactory(config=workers)

    def load_json(filename: str, validate: Optional[bool] = False) -> WorkerFactory:
        """Shortcut to load a worker configuration from a Json file.

        Parameters
        ----------
        filename: string
            Path to a Json file that contains the worker configuration.
        validate: bool, default=False
            Validate the given worker specifications against the `workerSpec`
            schema if True.

        Returns
        -------
        flowserv.controller.serial.worker.factory.WorkerFactory
        """
        with open(filename, 'r') as f:
            return WorkerFactory.load(doc=json.load(f), validate=validate)

    def load_yaml(filename: str, validate: Optional[bool] = False) -> WorkerFactory:
        """Shortcut to load a worker configuration from a Yaml file.

        Parameters
        ----------
        filename: string
            Path to a Yaml file that contains the worker configuration.
        validate: bool, default=False
            Validate the given worker specifications against the `workerSpec`
            schema if True.

        Returns
        -------
        flowserv.controller.serial.worker.factory.WorkerFactory
        """
        with open(filename, 'r') as f:
            return WorkerFactory.load(doc=yaml.load(f, Loader=yaml.FullLoader), validate=validate)
