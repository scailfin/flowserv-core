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
from typing import Dict, List, Optional, Union

import importlib.resources as pkg_resources
import json
import os

from flowserv.controller.worker.base import ContainerEngine
from flowserv.controller.worker.config import java_jvm, python_interpreter
from flowserv.controller.worker.docker import DockerWorker
from flowserv.controller.worker.subprocess import SubprocessWorker

import flowserv.util as util


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
    def __init__(
        self, config: Optional[Union[List, Dict]] = None,
        validate: Optional[bool] = False
    ):
        """Initialize the dictionary that contains the mapping of container
        image identifier to worker specification.

        If the configuration is a list of worker specification objects it will
        be converted into a dictionary.

        If the validate flag is True the given worker specifications are
        validated against the `workerSpec` schema.

        Parameters
        ----------
        config: list or dict, default=None
            List of worker specificatins or mapping of container image identifier
            to worker specifications that are used to create an instance of a
            :class:ContainerEngine worker.
        validate: bool, default=False
            Validate the given worker specifications against the `workerSpec`
            schema if True.
        """
        # If a list of worker specifications is given convert it to a mapping.
        if config and isinstance(config, list):
            config = convert_config(doc=config, validate=validate)
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
        flowserv.controller.worker.base.ContainerEngine
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
        flowserv.controller.worker.factory.WorkerFactory
        """
        return WorkerFactory(config=convert_config(doc=doc, validate=validate))

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
        flowserv.controller.worker.factory.WorkerFactory
        """
        doc = read_config(filename=filename, format=util.FORMAT_JSON, validate=validate)
        return WorkerFactory(config=doc)

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
        flowserv.controller.worker.factory.WorkerFactory
        """
        doc = read_config(filename=filename, format=util.FORMAT_YAML, validate=validate)
        return WorkerFactory(config=doc)


# -- Helper Functions for Worker configurations -------------------------------

def WorkerSpec(
    identifier: str, variables: Optional[Dict] = None, env: Optional[Dict] = None
) -> Dict:
    """Get a base configuration for a worker with the given arguments.

    Parameters
    ----------
    identifier: string
        Uniuqe worker type identifier.
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
    doc = {'worker': identifier}
    # Add optional mappings for template placeholders and environment variables
    # to thw worker configuration if given.
    args = dict()
    if variables:
        args['variables'] = variables
    if env:
        args['env'] = env
    if args:
        doc['arguments'] = args
    return doc


def Docker(variables: Optional[Dict] = None, env: Optional[Dict] = None) -> Dict:
    """Get base configuration for a subprocess worker with the given optional
    arguments.

    Parameters
    ----------
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
    return WorkerSpec(identifier='docker', variables=variables, env=env)


def Subprocess(variables: Optional[Dict] = None, env: Optional[Dict] = None) -> Dict:
    """Get base configuration for a subprocess worker with the given optional
    arguments.

    Parameters
    ----------
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
    return WorkerSpec(identifier='subprocess', variables=variables, env=env)


def convert_config(doc: List[Dict], validate: Optional[bool] = False) -> Dict:
    """Convertes a list of worker specifications into a dictionary.

    Parameters
    ----------
    doc: list of dict
        List of dictionaries that follow the `workerSpec` schema.
    validate: bool, default=False
        Validate the given worker specifications against the `workerSpec`
        schema if True.

    Returns
    -------
    dict
    """
    config = dict()
    for spec in doc:
        if validate:
            validator.validate(spec)
        config[spec['image']] = spec
    return config


def read_config(
    filename: str, format: Optional[str] = None, validate: Optional[bool] = False
) -> Dict:
    """Read worker configuration object from a given file.

    Parameters
    ----------
    filename: str
        Input file name
    format: string, optional
        Optional file format identifier.
    validate: bool, default=True
        Validate the given worker specifications against the `workerSpec`
        schema if True.

    Returns
    -------
    dict
    """
    return convert_config(doc=util.read_object(filename, format=format), validate=validate)
