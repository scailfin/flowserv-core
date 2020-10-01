# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for test runs of workflow templates."""

import os
import shutil
import tempfile

from typing import List, Optional, Tuple

from flowserv.app.base import App, install_app, open_app, uninstall_app
from flowserv.controller.serial.docker import DockerWorkflowEngine
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.model.auth import open_access
from flowserv.model.database import DB
from flowserv.model.files.fs import FileSystemStore
from flowserv.model.workflow.repository import WorkflowRepository

import flowserv.util as util


class Flowserv(object):
    """Environment for installing an running workflow templates. This class
    provides additional functionality for installing flowserv components. It
    is primarily intended for testing and/or  running flowserv in a notebook
    environment.

    The test environment will keep all workflow files in a folder on the file
    system. The environment uses SQLite as the database backend.

    The enviroment uses a serial workflow engine only at this point. The use
    can choose between running all workflows as separate processes in the local
    Python environment or using the Docker engine.
    """
    def __init__(
        self,
        basedir: Optional[str] = None,
        clear: Optional[bool] = False,
        use_docker: Optional[bool] = False,
        run_async: Optional[bool] = False
    ):
        """Initialize the components of the test environment.

        Parameters
        ----------
        basedir: string, default=None
            Base directory for all workflow files. If no directory is given a
            temporary directory will be created.
        clear: bool, default=False
            Remove all existing files and folders in the base directory if the
            clear flag is True.
        use_docker: bool, default=False
            Use Docker workflow engine.
        run_async: bool, default=False
            Run workflows in asynchronous mode.
        """
        # Set the base directory and ensure that it exists.
        self.basedir = basedir if basedir is not None else tempfile.mkdtemp()
        os.makedirs(self.basedir, exist_ok=True)
        # Remove all existing files and folders in the base directory if the
        # clear flag is True.
        if clear:
            util.cleardir(self.basedir)
        # Create a fresh database in the base directory.
        url = 'sqlite:///{}/flowserv.db'.format(os.path.abspath(self.basedir))
        self.db = DB(connect_url=url)
        self.db.init()
        # All files are stored on the file system in the base directory.
        self.fs = FileSystemStore(basedir=self.basedir)
        # Use an open access policy to avoid authentication errors.
        self.auth = open_access
        # Use a serial workflow engine. Either Docker or multi-process.
        if use_docker:
            self.engine = DockerWorkflowEngine(fs=self.fs, is_async=run_async)
        else:
            self.engine = SerialWorkflowEngine(fs=self.fs, is_async=run_async)

    def erase(self):
        """Delete the base folder for the test environment that contains all
        workflow files.
        """
        shutil.rmtree(self.basedir)

    def install(
        self,
        source: str, identifier: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        instructions: Optional[str] = None,
        specfile: Optional[str] = None,
        manifestfile: Optional[str] = None,
        ignore_postproc: Optional[bool] = False,

    ) -> App:
        """Create a new workflow in the environment that is defined by the
        template referenced by the source parameter. Returns the application
        object for the created workflow.

        Parameters
        ----------
        source: string
            Path to local template, name or URL of the template in the
            repository.
        identifier: string, default=None
            Unique user-provided workflow identifier. If no identifier is given
            a unique identifier will be generated for the application.
        name: string, default=None
            Unique workflow name
        description: string, default=None
            Optional short description for display in workflow listings
        instructions: string, default=None
            File containing instructions for workflow users.
        specfile: string, default=None
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)
        manifestfile: string, default=None
            Path to manifest file. If not given an attempt is made to read one
            of the default manifest file names in the base directory.
        ignore_postproc: bool, default=False
            Ignore post-processing workflow specification if True.

        Returns
        -------
        flowserv.app.base.App
        """
        workflow_id = install_app(
            source=source,
            identifier=identifier,
            name=name,
            description=description,
            instructions=instructions,
            specfile=specfile,
            manifestfile=manifestfile,
            ignore_postproc=ignore_postproc,
            db=self.db,
            fs=self.fs
        )
        return self.open(workflow_id)

    def open(self, identifier: str) -> App:
        """Get an instance of the floserv app for the workflow with the given
        identifier.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier.

        Returns
        -------
        flowserv.app.base.App
        """
        return open_app(
            identifier=identifier,
            db=self.db,
            engine=self.engine,
            fs=self.fs,
            auth=self.auth
        )

    def repository(self) -> List[Tuple[str, str]]:
        """Get list of tuples containing the identifier and description for
        each registered workflow template in the global repository.

        Returns
        -------
        list
        """
        return [(tid, desc) for tid, desc, _ in WorkflowRepository().list()]

    def uninstall(self, identifier: str):
        """Remove the workflow with the given identifier. This will also remove
        all run files for that workflow.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier.
        """
        uninstall_app(app_key=identifier, db=self.db, fs=self.fs)
