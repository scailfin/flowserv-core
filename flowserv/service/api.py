# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes method to create instances of the API components. All
components use the same underlying database connection. The connection object
is under the control of of a context manager to ensure that the connection is
closed properly after every API request has been handled.
"""

import os

from contextlib import contextmanager

from flowserv.core.db.driver import DatabaseDriver
from flowserv.model.group.manager import WorkflowGroupManager
from flowserv.model.ranking.manager import RankingManager
from flowserv.model.run.manager import RunManager
from flowserv.model.user.base import UserManager
from flowserv.model.user.auth import DefaultAuthPolicy
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.repo import WorkflowRepository
from flowserv.service.server import Service
from flowserv.service.user import UserService
from flowserv.service.workflow import WorkflowService
from flowserv.view.route import UrlFactory

import flowserv.config.api as config
import flowserv.core.error as err
import flowserv.core.util as util


"""Define the workflow backend as a global variable. This is necessary for the
multi-porcess backend to be able to maintain process state in between API
requests.
"""
# backend = config.FLOWSERV_ENGINE()


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
    def __init__(self, con, basedir=None, auth=None):
        """Initialize the database connection, URL factory, and the file system
        path generator. All other internal components are created when they are
        acccessed for the first time

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        basedir: string, optional
            Path to the base directory for the API
        auth: lowserv.model.user.auth.Auth, optional
            Authentication and authorization policy
        """
        self.con = con
        self.urls = UrlFactory()
        # Ensure that the API base directory exists
        fsdir = basedir if basedir is not None else config.API_BASEDIR()
        self.fs = WorkflowFileSystem(util.create_dir(fsdir))
        # Keep an instance of objects that may be used by multiple components
        # of the API. Use the respective property for each of them to ensure
        # that the object is instantiated before access.
        self._auth = auth if auth is not None else None
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
        flowserv.model.user.auth.Auth
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
        flowserv.model.user.base.UserHandle

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
        flowserv.model.group.manager.WorkflowGroupManager
        """
        if self._group_manager is None:
            self._group_manager = WorkflowGroupManager(
                con=self.con,
                fs=self.fs
            )
        return self._group_manager

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

    def service_descriptor(self, access_token):
        """Get the serialization of the service descriptor. The access token
        is verified to be active and to obtain the user name.

        Parameters
        ----------
        access_token: string
            API access token to authenticate the user

        Returns
        -------
        dict
        """
        try:
            username = self.authenticate(access_token).name
        except err.UnauthenticatedAccessError:
            username = None
        return Service().service_descriptor(username=username)

    def users(self):
        """Get instance of the user service component.

        Returns
        -------
        flowserv.service.user.UserService
        """
        if self._users is None:
            self._users = UserService(
                manager=UserManager(con=self.con),
                urls=self.urls
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
            workflow_repository=self.workflow_repository,
            ranking_manager=self.ranking_manager,
            urls=self.urls
        )

@contextmanager
def service():
    """The local service function is a context manager for an open database
    connection that is used to instantiate the API service class. The context
    manager ensures that the database conneciton in closed after an API request
    has been processed.

    Returns
    -------
    flowserv.service.api.API
    """
    con = DatabaseDriver.get_connector().connect()
    yield API(con)
    con.close()
