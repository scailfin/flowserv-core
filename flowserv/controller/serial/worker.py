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

from flowserv.controller.serial.result import ExecResult
from flowserv.controller.serial.workflow import ContainerStep
from flowserv.model.template.parameter import ParameterIndex

import flowserv.model.template.parameter as tp


class ContainerEngine(metaclass=ABCMeta):
    """Execution engine for container steps in a serial workflow. Provides the
    functionality to expand arguments in the individual command statements.
    Implementations may differ in the run method that executes the expanded
    commands.
    """
    def __init__(self, variables: Optional[Dict] = None):
        """Initialize the optional mapping with default values for placeholders
        in command template strings.

        Parameters
        ----------
        variables: dict, default=None
            Mapping with default values for placeholders in command template
            strings.
        """
        self.variables = variables if variables is not None else dict()

    def exec(
        self, step: ContainerStep, arguments: Dict, parameters: ParameterIndex,
        rundir: str
    ) -> ExecResult:
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
        parameters: lowserv.model.template.parameter.ParameterIndex
            Parameter declarations from the workflow template.
        rundir: string
            Path to the working directory of the workflow run.

        Returns
        -------
        flowserv.controller.serial.result.ExecResult
        """
        expanded_step = ContainerStep(image=step.image, env=step.env)
        for cmd in step.commands:
            cmd = tp.expand_value(
                value=cmd,
                arguments=arguments,
                parameters=parameters
            )
            # Generate mapping for template substitution. Include a mapping of
            # placeholder names to themselves.
            args = {p: p for p in tp.placeholders(cmd)}
            args.update(self.variables)
            args.update(arguments)
            expanded_step.add(Template(cmd).substitute(args))
        return self.run(step=expanded_step, rundir=rundir)

    @abstractmethod
    def run(self, step: ContainerStep, rundir: str) -> ExecResult:
        """Execute a list of commands in a workflow step.

        Parameters
        ----------
        step: flowserv.controller.serial.workflow.ContainerStep
            Step in a serial workflow.
        rundir: string
            Path to the working directory of the workflow run.

        Returns
        -------
        flowserv.controller.serial.result.ExecResult
        """
        raise NotImplementedError()  # pragma: no cover
