# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Execute a serial workflow for given user input arguments."""

from typing import List

from flowserv.controller.serial.workflow.result import RunResult
from flowserv.controller.worker.code import CodeWorker
from flowserv.controller.worker.factory import WorkerFactory
from flowserv.model.workflow.step import WorkflowStep
from flowserv.volume.manager import VolumeManager


def exec_workflow(
    steps: List[WorkflowStep], workers: WorkerFactory, volumes: VolumeManager,
    rundir: str, result: RunResult
) -> RunResult:
    """Execute steps in a serial workflow.

    The workflow arguments are part of the execution context that is contained
    in the :class:`flowserv.controller.serial.workflow.result.RunResult`. The
    result object is used to maintain the results for executed workflow steps.

    Executes workflow steps in sequence. Terminates early if the execution
    of a workflow step returns a non-zero value. Uses the given worker
    factory to create workers for steps that are of class
    :class:`flowserv.model.workflow.step.ContainerStep`.

    Parameters
    ----------
    steps: list of flowserv.model.workflow.step.WorkflowStep
        Steps in the serial workflow that are executed in the given context.
    workers: flowserv.controller.worker.factory.WorkerFactory, default=None
        Factory for :class:`flowserv.model.workflow.step.ContainerStep` steps.
    volumes: flowserv.controller.volume.manager.VolumeManager
        Manager for storage volumes that are used by the different workers.
    rundir: str, default=None
        Working directory for all executed workflow steps.
    result: flowserv.controller.worker.result.RunResult
        Collector for results from executed workflow steps. Contains the context
        within which the workflow is executed.

    Returns
    -------
    flowserv.controller.worker.result.RunResult
    """
    for step in steps:
        if step.is_function_step():
            worker = CodeWorker()
        else:
            worker = workers.get(step.image)
        # Prepare volume store that is associated with the worker.
        files = volumes.prepare(
            files=worker.inputfiles,
            stores=worker.stores,
            default_store=worker.default_store
        )
        # The volume store should provide the base directory that is being
        # passed to the worker's exec method.
        pass
        # Execute the workflow step.
        r = worker.exec(
            step=step,
            context=result.context,
            files=files,
            rundir=rundir
        )
        result.add(r)
        # Terminate if the step execution was not successful.
        if r.returncode != 0:
            break
        # Update volume manager with output files for the workflow step.
        volumes.update(files.worker.outputfiles, store=worker.default_store)
    return result
