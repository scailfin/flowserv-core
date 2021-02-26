# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serial workflow that executes a sequence of workflow steps.

Serial Workflows are either created from workflow templates that follow the
syntax of the REANA serial workflow specifications or explicitly within Python
scripts.
"""

from __future__ import annotations
from typing import Callable, Dict, Iterable, List, Optional

import os

from flowserv.controller.serial.engine.runner import exec_workflow
from flowserv.controller.serial.workflow.result import RunResult
from flowserv.model.workflow.step import FunctionStep, ContainerStep, WorkflowStep
from flowserv.controller.worker.factory import WorkerFactory
from flowserv.model.parameter.base import Parameter
from flowserv.model.template.parameter import ParameterIndex


class SerialWorkflow(object):
    """A serial workflow represents a sequence of steps (:class:WorkflowStep)
    that are executed in order for a given set of input parameters.

    At this point we distinguish two types of workflow steps: :class:FunctionStep
    and :class:ContainerStep.  A :class:FunctionStep is executed within the same
    thread and environment as the flowserv engine. A :class:ContainerStep is
    executed in a separate container-like environment. The execution environment
    is represented by a :clas:ContainerEngine that is associated in the
    :class:WorkerFactory with the environment identifier of the container step.
    """
    def __init__(
        self, steps: Optional[List[WorkflowStep]] = None,
        parameters: Optional[List[Parameter]] = None,
        workers: Optional[WorkerFactory] = None

    ):
        """Initialize the object properties.

        All properties are optional and can be initialized via different methods
        of the workflow instance.

        Parameters
        ----------
        steps: list of flowserv.model.workflow.step.WorkflowStep, default=None
            Optional sequence of steps in the serial workflow.
        parameters: list of flowserv.model.parameter.base.Parameter, default=None
            Optional list of workflow template parameters.
        workers: flowserv.controller.worker.factory.WorkerFactory
            Factory for :class:ContainerEngine objects that are used to execute
            individual :class:ContainerStep instances in the workflow sequence.
        """
        self.steps = steps if steps is not None else list()
        self.parameters = ParameterIndex(parameters=parameters)
        self.workers = workers if workers is not None else WorkerFactory()

    def __iter__(self) -> Iterable[WorkflowStep]:
        """Get an interator over the steps in the workflow.

        Returns
        -------
        itarable of flowserv.model.workflow.step.WorkflowStep
        """
        return iter(self.steps)

    def add_parameter(self, parameter: Parameter) -> SerialWorkflow:
        """Add a parameter to the internal index of workflow template parameters.

        Parameter identifier are expected to be unique. If a parameter with the
        same identifier as the given parameter already exists in the internal
        parameter index it will be replaced with the given parameter.

        Returns a reference to the object itself.

        Parameters
        ----------
        parameter: flowserv.model.parameter.base.Parameter
            Workflow termplate parameter that is added to the internal parameter
            index.

        Returns
        -------
        flowserv.controller.serial.workflow.base.SerialWorkflow
        """
        self.parameters[parameter.name] = parameter
        return self

    def add_step(
        self, image: Optional[str] = None, commands: Optional[List[str]] = None,
        env: Optional[Dict] = None, func: Optional[Callable] = None,
        output: Optional[str] = None, varnames: Optional[Dict] = None
    ) -> SerialWorkflow:
        """append a step to the workflow.

        Use this method to either add a code step or container step to the
        workflow. The method signature contains arguments for both types of
        steps. When calling the method only arguments for one of the two steps
        can be provided. Otherwise, a ValueError is raised.

        Parameters
        ----------
        image: string, default=None
            Execution environment identifier.
        commands: list(string), default=None
            List of command line statements.
        env: dict, default=None
            Environment variables for workflow step execution.
        func: callable, default=None
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

        Returns
        -------
        flowserv.controller.serial.workflow.base.SerialWorkflow
        """
        if image is not None and not func and not output and not varnames:
            return self.add_container_step(image=image, commands=commands, env=env)
        elif func is not None and not image and not commands and not env:
            return self.add_function_step(func=func, output=output, varnames=varnames)
        raise ValueError('invalid combination of arguments')

    def add_function_step(
        self, func: Callable, output: Optional[str] = None,
        varnames: Optional[Dict] = None
    ) -> SerialWorkflow:
        """Append a code step to the serial workflow.

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

        Returns
        -------
        flowserv.controller.serial.workflow.base.SerialWorkflow
        """
        step = FunctionStep(func=func, output=output, varnames=varnames)
        self.steps.append(step)
        return self

    def add_container_step(
        self, image: str, commands: Optional[List[str]] = None,
        env: Optional[Dict] = None
    ) -> SerialWorkflow:
        """Append a container step to the serial workflow.

        Parameters
        ----------
        image: string, default=None
            Execution environment identifier.
        commands: list(string), default=None
            List of command line statements.
        env: dict, default=None
            Environment variables for workflow step execution.

        Returns
        -------
        flowserv.controller.serial.workflow.base.SerialWorkflow
        """
        step = ContainerStep(image=image, commands=commands, env=env)
        self.steps.append(step)
        return self

    def run(
        self, arguments: Dict, workers: Optional[WorkerFactory] = None,
        rundir: Optional[str] = None
    ) -> RunResult:
        """Execute workflow for the given set of input arguments.

        Executes workflow steps in sequence. Terminates early if the execution
        of a workflow step returns a non-zero value. Uses the given worker
        factory to create workers for steps that are of class :class:ContainerStep.

        Collects results for all executed steps and returns them in the
        :class:RunResult.

        Parameters
        ----------
        arguments: dict
            User-provided arguments for the workflow run.
        workers: flowserv.controller.worker.factory.WorkerFactory, default=None
            Factory for :class:ContainerStep steps. Uses the default worker for
            all container steps if None.
        rundir: str, default=None
            Working directory for all executed workflow steps. Uses the current
            working directory if None.

        Returns
        -------
        flowserv.controller.worker.result.RunResult
        """
        # Use current working directory if run directory is None.
        rundir = rundir if rundir else os.getcwd()
        # Use default worker for all container steps if no factory is given.
        workers = workers if workers else WorkerFactory()
        # Execute the workflow and return the run result that contains the
        # results of the executed steps.
        return exec_workflow(
            steps=self.steps,
            workers=workers,
            rundir=rundir,
            result=RunResult(arguments=arguments)
        )
