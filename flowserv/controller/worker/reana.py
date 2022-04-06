# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow step processor that uses REANA to execute a given list of commands
as a serial REANA workflow.
"""

from typing import Dict, Optional

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.controller.worker.base import Worker
from flowserv.model.workflow.step import ContainerStep
from flowserv.volume.base import StorageVolume


"""Unique type identifier for REANAWorker serializations."""
REANA_WORKER = 'reana'


class REANAWorker(Worker):
    """Worker for container steps that uses the REANA backend.

    The REANA worker create a REANA workflow specification for a given
    container step and executes the workflow using a REANA backend.
    """
    def __init__(
        self, variables: Optional[Dict] = None, env: Optional[Dict] = None,
        identifier: Optional[str] = None, volume: Optional[str] = None
    ):
        """Initialize the optional mapping with default values for placeholders
        in command template strings.

        The default values for placeholder variables a fixed in the sense that
        they cannot be overriden by user-provided argument values.

        Parameters
        ----------
        variables: dict, default=None
            Mapping with fixed default values for placeholders in command
            template strings.
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
        super(REANAWorker, self).__init__(identifier=identifier, volume=volume)
        self.variables = variables if variables is not None else dict()
        self.env = env if env is not None else dict()

    def exec(self, step: ContainerStep, context: Dict, store: StorageVolume) -> ExecResult:
        """Execute a given list of commands that are represented by template
        strings.

        Substitutes parameter and template placeholder occurrences first. Then
        creates a REANA workflow specification, and runs the workflow on a
        REANA backend.

        Parameters
        ----------
        step: flowserv.controller.serial.workflow.ContainerStep
            Step in a serial workflow.
        context: dict
            Dictionary of argument values for parameters in the template.
        store: flowserv.volume.base.StorageVolume
            Storage volume that contains the workflow run files.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        print('Worker context')
        print(f"context={context}")
        print(f"variables={self.variables}")
        print(f"env={step.env}")
        print('\nWorkflow step')
        print(f"image={step.image}")
        print(f"commands={step.commands}")
        print(f"inputs={step.inputs}")
        print(f"outputs={step.outputs}")
        print('\nFile Storage')
        for filename, _ in store.walk(''):
            print(filename)
        # Add code that generates the REANA workflow template and executes it
        # here.
        raise NotImplementedError()
