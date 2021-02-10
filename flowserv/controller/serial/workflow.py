# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper class to execute workflow templates that follow the syntax of the
REANA serial workflow specifications.
"""

from __future__ import annotations
from typing import Callable, Dict, List, Optional
from string import Template

import inspect

from flowserv.model.template.base import WorkflowTemplate

import flowserv.model.template.parameter as tp


class WorkflowStep(object):
    """Base class for the different types of steps in a serial workflow. By now
    we distinguish between workflow steps that are executed in a container-like
    environment and steps that execute a given Python function.

    The aim of this base class is to provide functions to distinguish between
    the two step types.
    """
    def __init__(self, step_type: int):
        """Initialize the type identifier for the workflow step.

        Raises a ValueError if an invalid type identifier is given.

        Parameters
        ----------
        step_type: int
            Either 0 (ContainerStep) or 1 (CodeStep).
        """
        if step_type not in [0, 1]:
            raise ValueError("invalid step type '{}'".format(step_type))
        self.step_type = step_type

    def is_code_step(self) -> bool:
        """True if the workflow step is of type :class:CodeStep.

        Returns
        -------
        bool
        """
        return self.step_type == 1

    def is_container_step(self) -> bool:
        """True if the workflow step is of type :class:ContainerStep.

        Returns
        -------
        bool
        """
        return self.step_type == 0


class CodeStep(WorkflowStep):
    """Workflow step that executes a given Python function.

    The function is evaluated using the current state of the workflow arguments.
    If the executed function returns a result, the returned object can be added
    to the arguments. That is, the argument dictionary is updated and the added
    object is availble for the following workflows steps.
    """
    def __init__(
        self, func: Callable, output: Optional[str] = None,
        varnames: Optional[Dict] = None
    ):
        """Initialize the reference to the executed function and the optional
        return value target and variable name mapping.

        Parameters
        ----------
        func: callable
            Python function that is executed by the workflow step.
        output: string, default=None
            Name of the variable under which the function result is stored in
            the workflow arguments. If None, the function result is discarded.
        varnames: dict, default=None
            Mapping of function argument names to names of workflow arguments.
            This mapping is used when generating the arguments for the executed
            function. By default it is assumed that the names of arguments for
            the given function correspond to the names in the argument dictionary
            for the workflow. This mapping provides the option to map names in
            the function signature that do not occur in the arguments dictionary
            to argument names that are in the dictionary.
        """
        super(CodeStep, self).__init__(step_type=1)
        self.func = func
        self.output = output
        self.varnames = varnames if varnames is not None else dict()

    def exec(self, arguments: Dict):
        """Execute workflow step using the given arguments.

        The given set of input arguments may be modified by the return value of
        the evaluated function.

        Parameters
        ----------
        arguments: dict
            Mapping of parameter names to their current value in the workflow
            executon state.
        """
        # Generate argument dictionary from the signature of the evaluated function
        # and the variable name mapping.
        kwargs = dict()
        for var in inspect.getfullargspec(self.func).args:
            source = self.varnames.get(var, var)
            if source in arguments:
                kwargs[var] = arguments[source]
        # Evaluate the given function using the generated argument dictionary.
        result = self.func(**kwargs)
        # Add the function result to the argument dictionary if a variable name for
        # the result is given.
        if self.output is not None:
            arguments[self.output] = result


class ContainerStep(WorkflowStep):
    """Workflow step that is executed in a container environment. Contains a
    reference to the container identifier and a list of command line statements
    that are executed in a given environment.
    """
    def __init__(
        self, image: str, commands: Optional[List[str]] = None,
        env: Optional[Dict] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        image: string
            Execution environment identifier.
        commands: list(string), optional
            List of command line statements.
        env: dict, default=None
            Environment variables for workflow step execution.
        """
        super(ContainerStep, self).__init__(step_type=0)
        self.image = image
        self.commands = commands if commands is not None else list()
        self.env = env if env is not None else dict()

    def add(self, cmd: str) -> ContainerStep:
        """Append a given command line statement to the list of commands in the
        workflow step.

        Returns a reference to the object itself.

        Parameters
        ----------
        cmd: string
            Command line statement

        Returns
        -------
        flowserv.model.workflow.serial.Step
        """
        self.commands.append(cmd)
        return self


class SerialWorkflow(object):
    """Wrapper around a workflow template for serial workflow specifications
    that are following the basic structure of REANA serial workflows.

    The methods to get the list of commands, output files and upload files are
    modeled as properties to avoid confusion with the same properties for the
    remote workflow handle.
    """
    def __init__(
        self, template: WorkflowTemplate, arguments: dict, sourcedir: str
    ):
        """Initialize the object properties.

        Parameters
        ----------
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations
        arguments: dict
            Dictionary of argument values for parameters in the template. Maps
            the parameter identifier to the provided argument value.
        sourcedir: string
            Path to the source directory for template files.
        """
        self.template = template
        self.arguments = arguments
        self.sourcedir = sourcedir

    def commands(self) -> List[ContainerStep]:
        """Get expanded commands from template workflow specification. The
        commands within each step of the serial workflow specification are
        expanded for the given set of arguments and appended to the result
        list of commands.

        Returns
        -------
        list(flowserv.model.workflow.serial.Step)

        Raises
        ------
        flowserv.error.InvalidTemplateError
        flowserv.error.MissingArgumentError
        """
        workflow_spec = self.template.workflow_spec
        # Get the input parameters dictionary from the workflow specification
        # and replace all references to template parameters with the given
        # arguments or default values.
        workflow_parameters = tp.replace_args(
            spec=workflow_spec.get('inputs', {}).get('parameters', {}),
            arguments=self.arguments,
            parameters=self.template.parameters
        )
        # Add any workflow argument that is not contained in the modified
        # parameter list as a workflow parameter that is available for
        # replacement.
        for key in self.arguments:
            if key not in workflow_parameters:
                workflow_parameters[key] = str(self.arguments[key])
        # Add all command stings in workflow steps to result after replacing
        # references to parameters
        result = list()
        spec = workflow_spec.get('workflow', {}).get('specification', {})
        for step in spec.get('steps', []):
            env = tp.expand_value(
                value=step.get('environment'),
                arguments=workflow_parameters,
                parameters=self.template.parameters
            )
            script = ContainerStep(env=env)
            for cmd in step.get('commands', []):
                cmd = tp.expand_value(
                    value=cmd,
                    arguments=workflow_parameters,
                    parameters=self.template.parameters
                )
                script.add(Template(cmd).substitute(workflow_parameters))
            result.append(script)
        return result

    def output_files(self) -> List[str]:
        """Replace references to template parameters in the list of output
        files in the workflow specification.

        Returns
        -------
        list(string)

        Raises
        ------
        flowserv.error.InvalidTemplateError
        flowserv.error.MissingArgumentError
        """
        workflow_spec = self.template.workflow_spec
        return tp.replace_args(
            spec=workflow_spec.get('outputs', {}).get('files', {}),
            arguments=self.arguments,
            parameters=self.template.parameters
        )
