# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class for workers that execute workflow steps in different environments.
Implementations of the base class may execute workflow commands using the Docker
engine or the Python subprocess package.
"""

from abc import ABCMeta, abstractmethod
from string import Template
from typing import Dict, Optional

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.model.workflow.step import ContainerStep

import flowserv.model.template.parameter as tp


class ContainerEngine(metaclass=ABCMeta):
    """Execution engine for container steps in a serial workflow. Provides the
    functionality to expand arguments in the individual command statements.
    Implementations may differ in the run method that executes the expanded
    commands.
    """
    def __init__(self, variables: Optional[Dict] = None, env: Optional[Dict] = None):
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
        """
        self.variables = variables if variables is not None else dict()
        self.env = env if env is not None else dict()

    def exec(self, step: ContainerStep, arguments: Dict, rundir: str) -> ExecResult:
        """Execute a given list of commands that are represented by template
        strings.

        Substitutes parameter and template placeholder occurrences first. Then
        calls the implementation-specific run method to execute the individual
        commands.

        Parameters
        ----------
        step: flowserv.controller.serial.workflow.ContainerStep
            Step in a serial workflow.
        arguments: dict
            Dictionary of argument values for parameters in the template.
        rundir: string
            Path to the working directory of the workflow run.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        # Create a modified container step where all commands are expended so
        # that they do not contain references to variables and template parameters
        # any more.
        expanded_step = ContainerStep(image=step.image, env=step.env)
        for cmd in step.commands:
            # Generate mapping for template substitution. Include a mapping of
            # placeholder names to themselves.
            args = {p: p for p in tp.placeholders(cmd)}
            args.update(arguments)
            # Update arguments with fixed variables.
            args.update(self.variables)
            expanded_step.add(Template(cmd).substitute(args).strip())
        # Create mapping for environment variables.
        environment = dict(self.env)
        environment.update(step.env)
        environment = environment if environment else None
        return self.run(step=expanded_step, env=environment, rundir=rundir)

    @abstractmethod
    def run(self, step: ContainerStep, env: Dict, rundir: str) -> ExecResult:
        """Execute a list of commands in a workflow step.

        Parameters
        ----------
        step: flowserv.controller.serial.workflow.ContainerStep
            Step in a serial workflow.
        env: dict, default=None
            Default settings for environment variables when executing workflow
            steps. May be None.
        rundir: string
            Path to the working directory of the workflow run.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        raise NotImplementedError()  # pragma: no cover
