# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes method to create instances of the API components. All
components use the same underlying database connection. The connection object
is under the control of of a context manager to ensure that the connection is
closed properly after every API request has been handled.
"""

from contextlib import contextmanager

import logging

from flowserv.model.group import WorkflowGroupManager
from flowserv.model.ranking.manager import RankingManager
from flowserv.model.run.manager import RunManager
from flowserv.model.user import UserManager
from flowserv.model.auth import DefaultAuthPolicy
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.repo import WorkflowRepository
from flowserv.service.backend import init_backend
from flowserv.service.files import UploadFileService
from flowserv.service.group import WorkflowGroupService
from flowserv.service.run import RunService
from flowserv.service.server import Service
from flowserv.service.user import UserService
from flowserv.service.workflow import WorkflowService
from flowserv.view.factory import DefaultView

import flowserv.config.api as config
import flowserv.error as err
import flowserv.util as util


"""Name of the header element that contains the access token."""
HEADER_TOKEN = 'api_key'


"""Define the workflow backend as a global variable. This is necessary for the
multi-porcess backend to be able to maintain process state in between API
requests.
"""
backend = init_backend()


class API(object):
    """The API object implements a factory pattern for all API components. The
    individual components are instantiated on-demand to avoid any overhead for
    components that are not required to handle a user request.

    The API contains the following components:
    - groups()
    - runs()
    - service()
    - users()
    - workflows()
    """
    def __init__(self, con, engine=None, basedir=None, auth=None, view=None):
        """Initialize the database connection, URL factory, and the file system
        path generator. All other internal components are created when they are
        acccessed for the first time

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        engine: flowserv.controller.base.WorkflowController, optional
            Workflow controller used by the API for workflow execution
        basedir: string, optional
            Path to the base directory for the API
        auth: lowserv.model.user.auth.Auth, optional
            Authentication and authorization policy
        view: flowserv.view.factory.ViewFactory, optional
            Factory pattern for resource serializers
        """
        self.con = con
        # Use the global backend if no engine is specified
        self.engine = engine if engine is not None else backend
        # Ensure that the API base directory exists
        fsdir = basedir if basedir is not None else config.API_BASEDIR()
        self.fs = WorkflowFileSystem(util.create_dir(fsdir, abs=True))
        logging.info('API base directory {}'.format(fsdir))
        # Set the serializer factory
        self.view = view if view is not None else DefaultView()
        # Keep an instance of objects that may be used by multiple components
        # of the API. Use the respective property for each of them to ensure
        # that the object is instantiated before access.
        self._auth = auth
        self._engine = None
        self._group_manager = None
        self._ranking_manager = None
        self._run_manager = None
        self._workflow_repo = None
        self._users = None

    @property
    def auth(self):
        """Get authentication handler. The object is create only once.

        Returns
        -------
        flowserv.model.auth.Auth
        """
        if self._auth is None:
            self._auth = DefaultAuthPolicy(con=self.con)
        return self._auth

    def authenticate(self, access_token):
        """Authenticate the user based on the access token that is expected in
        the header of an API request. Returns the handle for the authenticated
        user.

        Raises an error if the access token is invalid.

        Parameters
        ----------
        access_token: string
            API access token to authenticate the user

        Returns
        -------
        flowserv.model.base.User

        Raises
        ------
        flowserv.error.UnauthenticatedAccessError
        """
        return self.auth.authenticate(access_token)

    @property
    def group_manager(self):
        """Get the group manager instance. The object is created when the
        manager is accessed for the first time.

        Returns
        --------
        flowserv.model.group..WorkflowGroupManager
        """
        if self._group_manager is None:
            self._group_manager = WorkflowGroupManager(
                con=self.con,
                fs=self.fs
            )
        return self._group_manager

    def groups(self):
        """Get API service component that provides functionality to access and
        manipulate workflows groups.

        Returns
        -------
        flowserv.service.group.WorkflowGroupService
        """
        return WorkflowGroupService(
            group_manager=self.group_manager,
            workflow_repo=self.workflow_repository,
            backend=self.engine,
            auth=self.auth,
            serializer=self.view.groups()
        )

    @property
    def ranking_manager(self):
        """Get the ranking manager instance. The object is created when the
        manager is accessed for the first time.

        Returns
        --------
        flowserv.model.ranking.manager.RankingManager
        """
        if self._ranking_manager is None:
            self._ranking_manager = RankingManager(con=self.con)
        return self._ranking_manager

    @property
    def run_manager(self):
        """Get the run manager instance. The object is created when the manager
        is accessed for the first time.

        Returns
        --------
        flowserv.model.run.manager.RunManager
        """
        if self._run_manager is None:
            self._run_manager = RunManager(
                con=self.con,
                fs=self.fs
            )
        return self._run_manager

    def runs(self):
        """Get API service component that provides functionality to access
        workflows runs.

        Returns
        -------
        flowserv.service.run.RunService
        """
        return RunService(
            run_manager=self.run_manager,
            group_manager=self.group_manager,
            workflow_repo=self.workflow_repository,
            ranking_manager=self.ranking_manager,
            backend=self.engine,
            auth=self.auth,
            serializer=self.view.runs()
        )

    def server(self, access_token=None):
        """Get API component for the service descriptor. The access token
        is verified to be active and to obtain the user name.

        Parameters
        ----------
        access_token: string, optional
            API access token to authenticate the user

        Returns
        -------
        flowserv.service.server.Service
        """
        try:
            username = self.authenticate(access_token).name
        except err.UnauthenticatedAccessError:
            username = None
        return Service(serializer=self.view.server(), username=username)

    def uploads(self):
        """Get API service component that provides functionality to access,
        delete, and upload files for workflows groups.

        Returns
        -------
        flowserv.service.files.UploadFileService
        """
        return UploadFileService(
            group_manager=self.group_manager,
            auth=self.auth,
            serializer=self.view.files()
        )

    def users(self):
        """Get instance of the user service component.

        Returns
        -------
        flowserv.service.user.UserService
        """
        if self._users is None:
            self._users = UserService(
                manager=UserManager(con=self.con),
                serializer=self.view.users()
            )
        return self._users

    @property
    def workflow_repository(self):
        """Get the workflow repository. The object is created when the
        repository is accessed for the first time.

        Returns
        --------
        flowserv.model.workflow.repo.WorkflowRepository
        """
        if self._workflow_repo is None:
            self._workflow_repo = WorkflowRepository(
                con=self.con,
                fs=self.fs
            )
        return self._workflow_repo

    def workflows(self):
        """Get API service component that provides functionality to access
        workflows and workflow leader boards.

        Returns
        -------
        flowserv.service.workflow.WorkflowService
        """
        return WorkflowService(
            workflow_repo=self.workflow_repository,
            ranking_manager=self.ranking_manager,
            serializer=self.view.workflows()
        )


@contextmanager
def service(engine=None):
    """The local service function is a context manager for an open database
    connection that is used to instantiate the API service class. The context
    manager ensures that the database conneciton in closed after an API request
    has been processed.

    Parameters
    ----------
    engine: flowserv.controller.base.WorkflowController, optional
        Workflow controller used by the API for workflow execution

    Returns
    -------
    flowserv.service.api.API
    """
    con = DatabaseDriver.get_connector().connect()
    yield API(con, engine=engine)
    con.close()
