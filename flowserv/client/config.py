# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper class to set values in a client configuration dictionary."""

from __future__ import annotations

import os

from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.auth import AUTH_OPEN, AUTH_DEFAULT, FLOWSERV_AUTH
from flowserv.config.client import FLOWSERV_CLIENT, LOCAL_CLIENT, REMOTE_CLIENT
from flowserv.config.database import FLOWSERV_DB
from flowserv.config.controller import FLOWSERV_ASYNC


class Config(dict):
    """Helper class that extends a dictionary with dedicated methods to set
    individual parameters in a client configuration.
    """
    def auth(self) -> Config:
        """Set the authentication method to the default value that requires
        authentication.

        Returns
        -------
        flowserv.client.config.Config
        """
        self[FLOWSERV_AUTH] = AUTH_DEFAULT
        return self

    def basedir(self, path: str) -> Config:
        """Set the flowserv base directory.

        Parameters
        ----------
        path: string
            Path to the base directory for all workflow files.

        Returns
        -------
        flowserv.client.config.Config
        """
        self[FLOWSERV_API_BASEDIR] = os.path.abspath(path)
        return self

    def database(self, url: str) -> Config:
        """Set the database connect Url.

        Parameters
        ----------
        url: string
            Database connect Url.

        Returns
        -------
        flowserv.client.config.Config
        """
        self[FLOWSERV_DB] = url
        return self

    def docker_engine(self) -> Config:
        return self

    def multiprocess_engine(self) -> Config:
        return self

    def multi_user(self) -> Config:
        """Set flag to indicate that workflow runs can be associated with
        different groups (submissions).

        Returns
        -------
        flowserv.client.config.Config
        """
        return self

    def open_access(self) -> Config:
        """Set the authentication method to open access.

        Returns
        -------
        flowserv.client.config.Config
        """
        self[FLOWSERV_AUTH] = AUTH_OPEN
        return self

    def run_async(self) -> Config:
        """Set the run asynchronous flag to True.

        Returns
        -------
        flowserv.client.config.Config
        """
        self[FLOWSERV_ASYNC] = 'True'
        return self

    def run_sync(self) -> Config:
        """Set the run asynchronous flag to False.

        Returns
        -------
        flowserv.client.config.Config
        """
        self[FLOWSERV_ASYNC] = 'False'
        return self

    def single_user(self) -> Config:
        return self
