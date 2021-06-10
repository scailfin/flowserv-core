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
from flowserv.model.workflow.step import CodeStep, ContainerStep, WorkflowStep
from flowserv.controller.worker.manager import WorkerPool
from flowserv.model.parameter.base import Parameter
from flowserv.model.template.parameter import ParameterIndex
from flowserv.volume.manager import DefaultVolume, VolumeManager


class SerialWorkflow(object):
    """A serial workflow represents a sequence of
    :class:`flowserv.model.workflow.step.WorkflowStep`) steps that are executed
    in order for a given set of input parameters.

    At this point we distinguish two types of workflow steps:
    :class:`flowserv.model.workflow.step.CodeStep`
    and :class:`flowserv.model.workflow.step.ContainerStep`.

    A :class:`flowserv.model.workflow.step.CodeStep` is executed within the
    same thread and environment as the flowserv engine. A
    :class:`flowserv.model.workflow.step.ContainerStep` is executed in a separate
    container-like environment. The execution environment is represented by a
    :class:`flowserv.controller.worker.base.ContainerWorker` that is associated
    in the :class:`flowserv.controller.worker.manager.WorkerPool` with the
    environment identifier of the container step.
    """
    def __init__(
        self, steps: Optional[List[WorkflowStep]] = None,
        parameters: Optional[List[Parameter]] = None,
        workers: Optional[WorkerPool] = None

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
        workers: flowserv.controller.worker.manager.WorkerPool
            Factory for :class:`flowserv.controller.worker.base.ContainerStep`
            objects that are used to execute individual
            :class:`flowserv.model.workflow.step.ContainerStep` instances in the
            workflow sequence.
        """
        self.steps = steps if steps is not None else list()
        self.parameters = ParameterIndex(parameters=parameters)
        self.workers = workers if workers is not None else WorkerPool()

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

    def add_code_step(
        self, identifier: str, func: Callable, arg: Optional[str] = None,
        varnames: Optional[Dict] = None, inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None
    ) -> SerialWorkflow:
        """Append a code step to the serial workflow.

        Parameters
        ----------
        identifier: str
            Unique workflow step identifier.
        func: callable
            Python function that is executed by the workflow step.
        arg: string, default=None
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
        inputs: list of string, default=None
            List of files that are required by the workflow step as inputs.
        outputs: list of string, default=None
            List of files that are generated by the workflow step as outputs.

        Returns
        -------
        flowserv.controller.serial.workflow.base.SerialWorkflow
        """
        step = CodeStep(
            identifier=identifier,
            func=func,
            arg=arg,
            varnames=varnames,
            inputs=inputs,
            outputs=outputs
        )
        self.steps.append(step)
        return self

    def add_container_step(
        self, identifier: str, image: str, commands: Optional[List[str]] = None,
        env: Optional[Dict] = None, inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None
    ) -> SerialWorkflow:
        """Append a container step to the serial workflow.

        Parameters
        ----------
        identifier: str
            Unique workflow step identifier.
        image: string, default=None
            Execution environment identifier.
        commands: list(string), default=None
            List of command line statements.
        env: dict, default=None
            Environment variables for workflow step execution.
        inputs: list of string, default=None
            List of files that are required by the workflow step as inputs.
        outputs: list of string, default=None
            List of files that are generated by the workflow step as outputs.

        Returns
        -------
        flowserv.controller.serial.workflow.base.SerialWorkflow
        """
        step = ContainerStep(
            identifier=identifier,
            image=image,
            commands=commands,
            env=env,
            inputs=inputs,
            outputs=outputs
        )
        self.steps.append(step)
        return self

    def run(
        self, arguments: Dict, workers: Optional[WorkerPool] = None,
        volumes: Optional[VolumeManager] = None
    ) -> RunResult:
        """Execute workflow for the given set of input arguments.

        Executes workflow steps in sequence. Terminates early if the execution
        of a workflow step returns a non-zero value. Uses the given worker
        factory to create workers for steps that are of class
        :class:`flowserv.model.workflow.step.ContainerStep`.

        Collects results for all executed steps and returns them in the
        :class:`flowserv.controller.serial.workflow.result.RunResult`.

        Parameters
        ----------
        arguments: dict
            User-provided arguments for the workflow run.
        workers: flowserv.controller.worker.manager.WorkerPool, default=None
            Factory for :class:`flowserv.model.workflow.step.ContainerStep`
            steps. Uses the default worker for all container steps if None.
        volumes: flowserv.volume.manager.VolumeManager
            Manager for storage volumes that are used by the different workers.

        Returns
        -------
        flowserv.controller.worker.result.RunResult
        """
        # Use current working directory as the default storage volume is no
        # volumes are specified.
        if volumes is None:
            volumes = DefaultVolume(basedir=os.getcwd())
        # Use default worker for all container steps if no factory is given.
        workers = workers if workers else WorkerPool()
        # Execute the workflow and return the run result that contains the
        # results of the executed steps.
        return exec_workflow(
            steps=self.steps,
            workers=workers,
            volumes=volumes,
            result=RunResult(arguments=arguments)
        )
