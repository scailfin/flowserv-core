# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Definitions for the different types of steps in a serial workflow. At this
point we distinguish two types of workflow steps: :class:FunctionStep and
:class:ContainerStep.

A :class:FunctionStep is used to execute a given function within the workflow context.
The code is executed within the same thread and environment as the flowserv
engine. Code steps are intended for minor actions (e.g., copying of files or
reading results from previous workflow steps). For these actions it would cause
too much overhead to create an external Python script that is run as a subprocess
or a Docker container image.

A :class:ContainerStep is a workflow step that is executed in a separate
container-like environment. The environment can either be a subprocess with
specific environment variable settings or a Docker container.
"""

from __future__ import annotations
from typing import Callable, Dict, List, Optional

import inspect


"""Unique identifier for workflow step types."""
CONTAINER_STEP = 'container'
FUNCTION_STEP = 'func'

STEPS = [CONTAINER_STEP, FUNCTION_STEP]


class WorkflowStep(object):
    """Base class for the different types of steps (actor) in a serial workflow.

    We distinguish several workflow steps including steps that are executed in
    a container-like environment and steps that directly execute Python code.

    The aim of this base class is to provide functions to distinguish between
    these two types of steps.
    """
    def __init__(self, step_type: int):
        """Initialize the type identifier for the workflow step.

        Raises a ValueError if an invalid type identifier is given.

        Parameters
        ----------
        step_type: int
            Either CONTAINER_STEP or FUNCTION_STEP.
        """
        if step_type not in STEPS:
            raise ValueError("invalid step type '{}'".format(step_type))
        self.step_type = step_type

    def is_container_step(self) -> bool:
        """True if the workflow step is of type :class:ContainerStep.

        Returns
        -------
        bool
        """
        return self.step_type == CONTAINER_STEP

    def is_function_step(self) -> bool:
        """True if the workflow step is of type :class:FunctionStep.

        Returns
        -------
        bool
        """
        return self.step_type == FUNCTION_STEP


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
        super(ContainerStep, self).__init__(step_type=CONTAINER_STEP)
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


class FunctionStep(WorkflowStep):
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
        super(FunctionStep, self).__init__(step_type=FUNCTION_STEP)
        self.func = func
        self.output = output
        self.varnames = varnames if varnames is not None else dict()

    def exec(self, context: Dict):
        """Execute workflow step using the given arguments.

        The given set of input arguments may be modified by the return value of
        the evaluated function.

        Parameters
        ----------
        context: dict
            Mapping of parameter names to their current value in the workflow
            executon state. These are the global variables in the execution
            context.
        """
        # Generate argument dictionary from the signature of the evaluated function
        # and the variable name mapping.
        kwargs = dict()
        for var in inspect.getfullargspec(self.func).args:
            source = self.varnames.get(var, var)
            if source in context:
                kwargs[var] = context[source]
        # Evaluate the given function using the generated argument dictionary.
        result = self.func(**kwargs)
        # Add the function result to the context dictionary if a variable name
        # for the result is given.
        if self.output is not None:
            context[self.output] = result
