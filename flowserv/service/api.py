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
from flowserv.model.user.base import UserManager
from flowserv.model.user.auth import DefaultAuthPolicy
from flowserv.service.server import Service
from flowserv.service.user import UserService
from flowserv.view.route import UrlFactory, HEADER_TOKEN

import flowserv.core.util as util


"""Define the workflow backend as a global variable. This is necessary for the
multi-porcess backend to be able to maintain process state in between API
requests.
"""
#backend = config.FLOWSERV_ENGINE()


class API(object):
    """The API object implements a factory pattern for all API components. The
    individual components are instantiated on-demand to avoid any overhead for
    components that are not required to handle a user request.

    The API contains the following components:
    - auth
    - engine
    - groups
    - rankings
    - runs
    - service
    - users
    - workflows
    """
    def __init__(self, con, auth=None):
        """Initialize the database connection and the URL factory. The URL
        factory is kept as a class property since every API component will
        have an instance of this class.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        auth: lowserv.model.user.auth.Auth, optional
            Authentication and authorization policy
        """
        self.con = con
        self.urls = UrlFactory()
        # Keep a copy of objects that may be used by multiple components of the
        # API. Use the respective get method for each of them to ensure that
        # the object is instantiated before access.
        self._auth = auth if auth is not None else None
        self._engine = None
        self._repo = None
        self._submissions = None
        self._users = None

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
        return self.auth().authenticate(access_token)

    def service(self, access_token):
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

    def submissions(self):
        """Get instance of the submission service component.

        Returns
        -------
        flowserv.service.submission.SubmissionService
        """
        return SubmissionService(
            engine=self.engine(),
            manager=self.submission_manager(),
            auth=self.auth(),
            repo=self.benchmark_repository(),
            urls=self.urls
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
                urls=self.urls
            )
        return self._users


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
