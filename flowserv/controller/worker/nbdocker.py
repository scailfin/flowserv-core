# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Worker for notebook steps that are executed using papermill inside a
Docker container.
"""

from typing import Dict, Optional

from flowserv.controller.serial.workflow.result import ExecResult
from flowserv.controller.worker.base import Worker
from flowserv.model.workflow.step import NotebookStep
from flowserv.volume.fs import FileSystemStorage


"""Unique type identifier for NotebookDockerWorker serializations."""
NOTEBOOK_DOCKER_WORKER = 'nbdocker'


class NotebookDockerWorker(Worker):
    """Execution engine for notebook steps in a serial workflow."""
    def __init__(
        self, env: Optional[Dict] = None, identifier: Optional[str] = None,
        volume: Optional[str] = None
    ):
        """Initialize the worker identifier and accessible storage volume.

        Parameters
        ----------
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
        super(NotebookDockerWorker, self).__init__(identifier=identifier, volume=volume)
        self.env = env

    def exec(self, step: NotebookStep, context: Dict, store: FileSystemStorage) -> ExecResult:
        """Execute a given notebook workflow step in the current workflow
        context.

        The notebook engine expects a file system storage volume that provides
        access to the notebook file and any other aditional input files.

        Parameters
        ----------
        step: flowserv.model.workflow.step.NotebookStep
            Notebook step in a serial workflow.
        context: dict
            Dictionary of variables that represent the current workflow state.
        store: flowserv.volume.fs.FileSystemStorage
            Storage volume that contains the workflow run files.

        Returns
        -------
        flowserv.controller.serial.workflow.result.ExecResult
        """
        # Create Docker image including papermill and notebook requirements.
        """
        FROM python:3.9
        COPY requirements.txt /app/requirements.txt
        WORKDIR /app
        RUN pip install papermill
        RUN pip install -r requirements.txt
        RUN rm -Rf /app
        WORKDIR /
        """
        pass
        # Run notebook in Docker container.
        pass
