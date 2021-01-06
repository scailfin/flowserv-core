# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper method to create a API generator based on the current configuration
in the environment valriables.
"""

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from typing import Dict, Optional, Union

import os

from flowserv.config.api import API_DEFAULTDIR, FLOWSERV_API_BASEDIR
from flowserv.config.auth import AUTH_OPEN, DEFAULT_USER, FLOWSERV_AUTH
from flowserv.config.base import get_variable as env
from flowserv.config.client import FLOWSERV_CLIENT, LOCAL_CLIENT, REMOTE_CLIENT
from flowserv.config.database import FLOWSERV_DB
from flowserv.config.controller import FLOWSERV_ASYNC
from flowserv.model.database import DB
from flowserv.service.api import API
from flowserv.service.local import service as local_service

import flowserv.config.client as config


# -- API factory for the command-line interface -------------------------------

@contextmanager
def service():
    """
    """
    os.environ[FLOWSERV_ASYNC] = 'False'
    with local_service(access_token=config.ACCESS_TOKEN()) as api:
        yield api


# -- API factory pattern for client applications ------------------------------

class APIFactory(metaclass=ABCMeta):
    """Interface for API factories. Defines a single method that is used to
    create a new instance of a local or remote service API.
    """
    @abstractmethod
    def api(self) -> API:
        """Create a new instance of the service API.

        Returns
        -------
        flowserv.service.api.API
        """
        raise NotImplementedError()


class LocalAPIFactory(APIFactory):
    """The API factory is a context manager for creating instances of the local
    service API based on a given set of configuration parameters.

    The following environment variables are considered for a local setting:

    - FLOWSERV_API_DIR
    - FLOWSERV_DATABASE
    """
    def __init__(self, config: Dict):
        """Initialize the configuration parameters.

        Parameters
        ----------
        config: dict
            Dictionary with configuration parameter values.
        """
        self.config = config
        # Get the base directory for workflow files.
        if FLOWSERV_API_BASEDIR not in self.config:  # pragma: no cover
            # Use the default flowserv directory in the user HOME directory
            # if the base directory is not set.
            basedir = env(FLOWSERV_API_BASEDIR, default=API_DEFAULTDIR())
            self.config[FLOWSERV_API_BASEDIR] = basedir
        # Get the local database instance object..
        if FLOWSERV_DB not in self.config:
            # Use a SQLite database in the dabase directory as default.
            # This database needs to be initialized if it does not exist.
            dbfile = '{}/flowserv.db'.format(self.config[FLOWSERV_API_BASEDIR])
            default_url = 'sqlite:///{}'.format(dbfile)
            self.config[FLOWSERV_DB] = env(FLOWSERV_DB, default=default_url)
            url = self.config[FLOWSERV_DB]
            # Maintain a reference to the local database instance for use
            # when creating API instances.
            self._db = DB(connect_url=url)
            if url == default_url and not os.path.isfile(dbfile):
                # Initialize the database if the database if the configuration
                # references the default database and the database file does
                # not exist.
                self._db.init()
        else:
            self._db = DB(connect_url=self.config[FLOWSERV_DB])
        # The initial value for the user identifier is dependent on the
        # authentication policy. If open access is used (which is the default
        # in this setting), a default user identifier is set. Otherwise, the
        # user identifier is None until the user logs in.
        if FLOWSERV_AUTH not in self.config:
            # Use open access as the default authentication policy.
            self.config[FLOWSERV_AUTH] = env(FLOWSERV_AUTH, AUTH_OPEN)
        self._user_id = DEFAULT_USER if self.config[FLOWSERV_AUTH] == AUTH_OPEN else None
        # Set the asynchronous flag in the environment if specified in the
        # configuration.
        if FLOWSERV_ASYNC in self.config:
            os.environ[FLOWSERV_ASYNC] = self.config[FLOWSERV_ASYNC]

    def api(self) -> API:
        """Create a new instance of the local service API.

        Returns
        -------
        flowserv.service.api.API
        """
        return local_service(db=self._db, user_id=self._user_id)


class RemoteAPIFactory(APIFactory):
    """
    """
    def __init__(self, config: Dict):
        """
        """
        pass


def api_factory(config: Optional[Dict] = None) -> Union[LocalAPIFactory, RemoteAPIFactory]:
    """Create an instance of the API factory that is responsible for generating
    API instances for a flowserv client.

    The main distinction here is whether a connection is made to a local instance
    of the service or to a remote instance. This distinction is made based on
    the value of the FLOWSERV_CLIENT environment variable that takes the values
    'local' or 'remote'. The default is 'local'.

    Parameters
    ----------
    config: dict, default=None
        Dictionary with configuration parameter values.

    Returns
    -------
    flowserv.client.api.LocalAPIFactory or flowserv.client.api.RemoteAPIFactory
    """
    config = config if config is not None else dict()
    # Get the factory class instance based on the client type. Raises an error
    # if an invalid client type is specified.
    client = config.get(FLOWSERV_CLIENT, LOCAL_CLIENT)
    if client == LOCAL_CLIENT:
        return LocalAPIFactory(config)
    elif client == REMOTE_CLIENT:
        return RemoteAPIFactory(config)
    else:
        raise ValueError("invalid client type '{}'".format(client))
