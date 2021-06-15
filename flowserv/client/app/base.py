# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for test runs of workflow templates."""

from typing import Dict, List, Optional

import shutil
import tempfile

from flowserv.client.api import ClientAPI
from flowserv.client.app.workflow import Workflow
from flowserv.model.parameter.base import Parameter
from flowserv.view.group import GROUP_ID
from flowserv.view.user import USER_ID

import flowserv.config as config
import flowserv.util as util
import flowserv.view.workflow as labels


class Flowserv(object):
    """Client environment for interacting with a flowserv instance. This class
    provides additional functionality for installing flowserv applications. It
    is primarily intended running flowserv in programming environments, e.g.,
    Jupyter Notebooks.
    """
    def __init__(
        self, env: Optional[Dict] = None, basedir: Optional[str] = None,
        database: Optional[str] = None, open_access: Optional[bool] = None,
        run_async: Optional[bool] = None, clear: Optional[bool] = False,
        user_id: Optional[str] = None
    ):
        """Initialize the client API factory. Provides the option to alter the
        default settings of environment variables and for using custom instance
        of the database and workflow engine.

        Parameters
        ----------
        env: dict, default=None
            Dictionary with configuration parameter values.
        basedir: string, default=None
            Base directory for all workflow files. If no directory is given or
            specified in the environment a temporary directory will be created.
        database: str, default=None
            Databse connection Url.
        open_access: bool, default=None
            Use an open access policy if set to True.
        run_async: bool, default=False
            Run workflows in asynchronous mode.
        clear: bool, default=False
            Remove all existing files and folders in the base directory if the
            clear flag is True.
        user_id: string, default=None
            Identifier for an authenticated default user.
        """
        # Get the base configuration settings from the environment if not given.
        self.env = env if env is not None else config.env()
        # Set the base directory and ensure that it exists. Create a temporary
        # directory if no base directory is specified. If a base directory was
        # specified by the user it will override the settings from the global
        # environment.
        basedir = basedir if basedir is not None else self.env.get(config.FLOWSERV_BASEDIR)
        self.basedir = basedir if basedir is not None else tempfile.mkdtemp()
        self.env[config.FLOWSERV_BASEDIR] = self.basedir
        # Remove all existing files and folders in the base directory if the
        # clear flag is True.
        if clear:
            util.cleardir(self.basedir)
        self.service = ClientAPI(
            env=self.env,
            basedir=self.basedir,
            database=database,
            open_access=open_access,
            run_async=run_async,
            user_id=user_id
        )

    def create_submission(
        self, workflow_id: str, name: str, members: Optional[List[str]] = None,
        parameters: Optional[List[Parameter]] = None, engine_config: Optional[Dict] = None
    ) -> str:
        """Create a new user group for a given workflow. Each group has a
        a unique name for the workflow, a list of additional group members, and
        a specification of additional parameters. The parameters allow to define
        variants of the original workflow template.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        name: string
            Unique team name
        members: list(string), default=None
            List of user identifier for group members
        parameters: list of flowserv.model.parameter.base.Parameter, default=None
            Optional list of parameter declarations that are used to modify the
            template parameters for submissions of the created group.
        engine_config: dict, default=None
            Optional configuration settings that will be used as the default
            when running a workflow.

        Returns
        -------
        string
        """
        with self.service() as api:
            doc = api.groups().create_group(
                workflow_id=workflow_id,
                name=name,
                members=members,
                parameters=parameters,
                engine_config=engine_config
            )
        return doc[GROUP_ID]

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
        engine_config: Optional[Dict] = None,
        ignore_postproc: Optional[bool] = False,
        multi_user: Optional[bool] = False,
        verbose: Optional[bool] = False
    ) -> str:
        """Create a new workflow in the environment that is defined by the
        template referenced by the source parameter. Returns the identifier
        of the created workflow.

        If the multi user flag is False this method will also create a group
        with the same identifier as the workflow.

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
        engine_config: dict, default=None
            Optional configuration settings that will be used as the default
            when running a workflow.
        ignore_postproc: bool, default=False
            Ignore post-processing workflow specification if True.
        multi_user: bool, default=False
            If the multi user flag is False a group will be created for the
            workflow with the same identifier as the workflow.
        verbose: bool, default=False
            Print information about source and target volume and the files that
            are being copied.

        Returns
        -------
        string
        """
        with self.service() as api:
            doc = api.workflows().create_workflow(
                source=source,
                identifier=identifier,
                name=name,
                description=description,
                instructions=instructions,
                specfile=specfile,
                manifestfile=manifestfile,
                engine_config=engine_config,
                ignore_postproc=ignore_postproc,
                verbose=verbose
            )
            workflow_id = doc[labels.WORKFLOW_ID]
            if not multi_user:
                api.groups().create_group(
                    workflow_id=workflow_id,
                    name=workflow_id,
                    engine_config=engine_config,
                    identifier=workflow_id
                )
        return workflow_id

    def login(self, username: str, password: str):
        """Authenticate the user using the given credentials.

        Parameters
        ----------
        username: string
            Unique user name
        password: string
            User password
        """
        self.service.login(username=username, password=password)

    def logout(self):
        """Logout the currently authenticated user."""
        self.service.logout()

    def open(self, identifier: str) -> Workflow:
        """Get an instance of the floserv app for the workflow with the given
        identifier.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier.

        Returns
        -------
        flowserv.client.app.workflow.Workflow
        """
        return self.submission(workflow_id=identifier, group_id=identifier)

    def register(self, username: str, password: str) -> str:
        """Register a new user with the given credentials.

        Parameters
        ----------
        username: string
            Unique user name.
        password: string
            User password.

        Returns
        -------
        string
        """
        with self.service() as api:
            doc = api.users().register_user(
                username=username,
                password=password,
                verify=False
            )
        return doc[USER_ID]

    def submission(self, workflow_id: str, group_id: str) -> Workflow:
        """Get the handle for a workflow with a given identifier and for a
        given user group.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier.
        group_id: string
            Unique user group identifier.

        Returns
        -------
        flowserv.client.app.workflow.Workflow
        """
        return Workflow(
            workflow_id=workflow_id,
            group_id=group_id,
            service=self.service
        )

    def uninstall(self, identifier: str):
        """Remove the workflow with the given identifier. This will also remove
        all run files for that workflow.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier.
        """
        with self.service() as api:
            api.workflows().delete_workflow(workflow_id=identifier)
