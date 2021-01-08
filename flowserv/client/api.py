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
from typing import Callable, Dict, Optional, Union

import os

from flowserv.model.database import DB
from flowserv.service.api import API
from flowserv.service.local import service as local_service

import flowserv.config as config


# -- API factory pattern for client applications ------------------------------



class LocalAPIFactory(APIFactory):
    """The API factory is a factory pattern for creating instances of the local
    service API based on a given set of configuration parameters.

    The following environment variables are considered for a local setting:

    - FLOWSERV_API_DIR: The base directory for all workflow files. By default
      a folder in the users HOME directory is used as the base directory.
    - FLOWSERV_DATABASE: Database connection Url. By default a SQLite database
      in the base directory will be used.
    - FLOWSERV_AUTH: Specification of the authentication policy. By default an
      open access policy is used.
    """
    def __init__(self, env: Dict):
        """Initialize the configuration parameters.

        Parameters
        ----------
        env: dict
            Dictionary with configuration parameter values.
        """
        # Get the base directory for workflow files.
        if config.FLOWSERV_API_BASEDIR not in env:  # pragma: no cover
            # Use the default flowserv directory in the user HOME directory
            # if the base directory is not set.
            basedir = env(FLOWSERV_API_BASEDIR, default=API_DEFAULTDIR())
            env[FLOWSERV_API_BASEDIR] = basedir
        # Get the local database instance object..
        if FLOWSERV_DB not in env:
            # Use a SQLite database in the dabase directory as default.
            # This database needs to be initialized if it does not exist.
            dbfile = '{}/flowserv.db'.format(env[FLOWSERV_API_BASEDIR])
            default_url = 'sqlite:///{}'.format(dbfile)
            env[FLOWSERV_DB] = env(FLOWSERV_DB, default=default_url)
            url = env[FLOWSERV_DB]
            # Maintain a reference to the local database instance for use
            # when creating API instances.
            self._db = DB(connect_url=url)
            if url == default_url and not os.path.isfile(dbfile):
                # Initialize the database if the database if the configuration
                # references the default database and the database file does
                # not exist.
                self._db.init()
        else:
            self._db = DB(connect_url=env[FLOWSERV_DB])
        # The initial value for the user identifier is dependent on the
        # authentication policy. If open access is used (which is the default
        # in this setting), a default user identifier is set. Otherwise, the
        # user identifier is None until the user logs in.
        if FLOWSERV_AUTH not in env:
            # Use open access as the default authentication policy.
            env[FLOWSERV_AUTH] = env(FLOWSERV_AUTH, AUTH_OPEN)
        self._user_id = DEFAULT_USER if env[FLOWSERV_AUTH] == AUTH_OPEN else None
        # Set the asynchronous flag in the environment if specified in the
        # configuration.
        if FLOWSERV_ASYNC in env:
            os.environ[FLOWSERV_ASYNC] = env[FLOWSERV_ASYNC]

    def api(self) -> Callable:
        """Return an instance of the context manager for creating new instance
        of the local service API.

        Returns
        -------
        contextmanager
        """
        return local_service(db=self._db, user_id=self._user_id)


class RemoteAPIFactory(APIFactory):
    """
    """
    def __init__(self, env: Dict):
        """
        """
        pass

    def api(self) -> Callable:
        """Create a context manager that yields a new instance of the service
        API.

        Returns
        -------
        contextmanager
        """
        raise NotImplementedError()


def api_factory(env: Optional[Dict] = None) -> APIFactory:
    """Create an instance of the API factory that is responsible for generating
    API instances for a flowserv client.

    The main distinction here is whether a connection is made to a local instance
    of the service or to a remote instance. This distinction is made based on
    the value of the FLOWSERV_CLIENT environment variable that takes the values
    'local' or 'remote'. The default is 'local'.

    Parameters
    ----------
    env: dict, default=None
        Dictionary with configuration parameter values.

    Returns
    -------
    flowserv.client.api.APIFactory
    """
    # Initialize the configuration from the environment if not given.
    env = env if config is not None else config.env()
    # Get the factory class instance based on the client type. Raises an error
    # if an invalid client type is specified.
    client = config.get(config.FLOWSERV_CLIENT, config.LOCAL_CLIENT)
    if client == config.LOCAL_CLIENT:
        return LocalAPIFactory(env=env)
    elif client == config.REMOTE_CLIENT:
        return RemoteAPIFactory(env=env)
    raise ValueError("invalid client type '{}'".format(client))


# -- API factory for the command-line interface -------------------------------

def cliservice(env: Optional[Dict] = None) -> APIFactory:
    """
    """
    # Initialize the configuration from the environment if not given.
    env = env if config is not None else config.env()
    # Ensure that all workflows are run asynchronously.
    env.run_sync()
    # Return API context manager from the API factory that is specified in the
    # configuration.
    return api_factory(env=env).api()
