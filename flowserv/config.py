# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of configuration parameters for different components of flowserv.

The module contains helper classes to get configuration values from environment
variables and to customize the configuration settings.
"""

from __future__ import annotations
from appdirs import user_cache_dir
from typing import Any, Dict, Optional, Union

import os

from flowserv.volume.fs import FStore

import flowserv.error as err
import flowserv.util as util

# --
# -- Environment variables and default values
# --

# -- API ----------------------------------------------------------------------

"""Names of environment variables that are used to configure the API."""
# Base directory to store uploaded files and submission results
FLOWSERV_BASEDIR = 'FLOWSERV_API_DIR'
# Host name for API server
FLOWSERV_API_HOST = 'FLOWSERV_API_HOST'
# Name of the API instance
FLOWSERV_API_NAME = 'FLOWSERV_API_NAME'
# API application path
FLOWSERV_API_PATH = 'FLOWSERV_API_PATH'
# API server port on host
FLOWSERV_API_PORT = 'FLOWSERV_API_PORT'
# Url protocol prefix
FLOWSERV_API_PROTOCOL = 'FLOWSERV_API_PROTOCOL'


"""Default values for environment variables."""
DEFAULT_HOST = 'localhost'
DEFAULT_NAME = 'Reproducible and Reusable Data Analysis Workflow Server (API)'
DEFAULT_PATH = '/flowserv/api/v1'
DEFAULT_PORT = 5000
DEFAULT_PROTOCOL = 'http'


def API_DEFAULTDIR() -> str:
    """The default API base directory is a subfolder in the users data cache
    directory.

    The default user cache directory is different for different OS's. We use
    appdirs.user_cache_dir to get the directory.

    Returns
    -------
    string
    """
    return user_cache_dir(appname=__name__.split('.')[0])


def API_URL(env: Dict) -> str:
    """Get the base URL for the API from the respective environment variables
    'FLOWSERV_API_HOST', 'FLOWSERV_API_PATH', and 'FLOWSERV_API_PORT' in the
    given configuration dictionary.

    Parameters
    ----------
    env: dict
        Configuration object that provides access to configuration
        parameters in the environment.

    Returns
    -------
    string
    """
    protocol = env.get(FLOWSERV_API_PROTOCOL, DEFAULT_PROTOCOL)
    host = env.get(FLOWSERV_API_HOST, DEFAULT_HOST)
    port = to_int(env.get(FLOWSERV_API_PORT, DEFAULT_PORT))
    if port != 80:
        host = '{}:{}'.format(host, port)
    path = env.get(FLOWSERV_API_PATH, DEFAULT_PATH)
    if not path.startswith('/'):
        path = '/' + path
    return '{}://{}{}'.format(protocol, host, path)


# -- Application --------------------------------------------------------------

"""Names of environment variables that configure the application."""

FLOWSERV_APP = 'FLOWSERV_APP'
FLOWSERV_GROUP = 'FLOWSERV_GROUP'

# Respective variable names for ROB.
ROB_BENCHMARK = 'ROB_BENCHMARK'
ROB_SUBMISSION = 'ROB_SUBMISSION'


def APP() -> str:
    """Get the value for the FLOWSERV_APP variable from the environment. Raises
    a missing configuration error if the value is not set.
    Returns
    -------
    string
    """
    app_key = os.environ.get(FLOWSERV_APP)
    if not app_key:
        raise err.MissingConfigurationError('workflow identifier')
    return app_key


# -- Auth ---------------------------------------------------------------------

"""Names of environment variables that are used to configure the authentication
module.
"""
# Access token for the command line interface
FLOWSERV_ACCESS_TOKEN = 'FLOWSERV_ACCESS_TOKEN'
# Time period for which an API key is valid
FLOWSERV_AUTH_LOGINTTL = 'FLOWSERV_AUTH_TTL'
# Authentication policy
FLOWSERV_AUTH = 'FLOWSERV_AUTH'


"""Default values for environment variables."""
DEFAULT_LOGINTTL = 24 * 60 * 60
# Access policies
AUTH_DEFAULT = 'default'
AUTH_OPEN = 'open'

"""Default user."""
DEFAULT_USER = '0' * 8


# -- Backend ------------------------------------------------------------------

"""Environment variables that control the configuration of the workflow
controllers.
"""
# Name of the class that implements the workflow controller interface
FLOWSERV_BACKEND_CLASS = 'FLOWSERV_BACKEND_CLASS'
# Name of the module that contains the workflow controller implementation
FLOWSERV_BACKEND_MODULE = 'FLOWSERV_BACKEND_MODULE'
# Flag indicating whether workflows are executed asynchronously or blocking.
FLOWSERV_ASYNC = 'FLOWSERV_ASYNCENGINE'
DEFAULT_ASYNC = 'True'

# Poll interval
FLOWSERV_POLL_INTERVAL = 'FLOWSERV_POLLINTERVAL'
DEFAULT_POLL_INTERVAL = 2


# -- Client -------------------------------------------------------------------

"""Environment variables for the command line interface."""
# Define the type of client that the command line interface uses.
FLOWSERV_CLIENT = 'FLOWSERV_CLIENT'
LOCAL_CLIENT = 'local'
REMOTE_CLIENT = 'remote'


# -- Database -----------------------------------------------------------------

"""Environment variable that contains the database connection string."""
FLOWSERV_DB = 'FLOWSERV_DATABASE'
FLOWSERV_WEBAPP = 'FLOWSERV_WEBAPP'


# -- File store ---------------------------------------------------------------

"""Environment variables that are used to configure the file store that is used
to maintain files for workflow templates, user group uploads, and workflow runs.
"""
# Identifier of the file store class.
FLOWSERV_FILESTORE = 'FLOWSERV_FILESTORE'


# --
# -- Configuration settings
# --


class Config(dict):
    """Helper class that extends a dictionary with dedicated methods to set
    individual parameters in the flowserv configuration.

    Methods with lower case names are setters for configuration parameters. All
    setters return a reference to the configuration object itself to allows
    chanining the setter calls.
    """
    def __init__(self, defaults: Optional[Dict] = None):
        """Initialize the dictionary.

        Parameters
        ----------
        defaults: dict, default=None
            Dictionary with default settings.
        """
        if defaults is not None:
            super(Config, self).__init__(**defaults)

    def auth(self) -> Config:
        """Set the authentication method to the default value that requires
        authentication.

        Returns
        -------
        flowserv.config.Config
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
        flowserv.config.Config
        """
        self[FLOWSERV_BASEDIR] = os.path.abspath(path)
        return self

    def database(self, url: str) -> Config:
        """Set the database connect Url.

        Parameters
        ----------
        url: string
            Database connect Url.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_DB] = url
        return self

    def multiprocess_engine(self) -> Config:
        """Set configuration to use the serial multi-porcess workflow controller
        as the default backend.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_BACKEND_MODULE] = 'flowserv.controller.serial.engine.base'
        self[FLOWSERV_BACKEND_CLASS] = 'SerialWorkflowEngine'
        return self

    def open_access(self) -> Config:
        """Set the authentication method to open access.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_AUTH] = AUTH_OPEN
        return self

    def run_async(self) -> Config:
        """Set the run asynchronous flag to True.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_ASYNC] = True
        return self

    def run_sync(self) -> Config:
        """Set the run asynchronous flag to False.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_ASYNC] = False
        return self

    def token_timeout(self, timeout: int) -> Config:
        """Set the authentication token timeout interval.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_AUTH_LOGINTTL] = timeout
        return self

    def volume(self, config: dict) -> Config:
        """Set configuration object for the file storage volume.

        Parameters
        ----------
        config: dict
            Volume configuration information for the storage volume factory.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_FILESTORE] = config
        return self

    def webapp(self) -> Config:
        """Set the web app flag to True.

        Returns
        -------
        flowserv.config.Config
        """
        self[FLOWSERV_WEBAPP] = True
        return self


# -- Initialize configuration from environment variables ----------------------

def to_bool(value: Any) -> bool:
    """Convert given value to Boolean. Only of the string matches 'True' (ignoring
    case) the result will be True.

    Parameters
    ----------
    value: any
        Expects a string representation of a Boolean value.

    Returns
    -------
    bool
    """
    try:
        return True if value.lower() == 'true' else False
    except AttributeError:
        return False


def to_float(value: Any) -> float:
    """Convert given value to float.

    Parameters
    ----------
    value: any
        Expects an integer, float, or a string representation of an integer
        or float value.

    Returns
    -------
    int
    """
    try:
        return float(value)
    except ValueError:
        return None


def to_int(value: Any) -> int:
    """Convert given value to integer.

    Parameters
    ----------
    value: any
        Expects an integer value or a string representation of an integer value.

    Returns
    -------
    int
    """
    try:
        return int(value)
    except ValueError:
        return None


def read_config_obj(filename: Union[str, Dict]) -> Dict:
    """Read configuration object from a file.

    This function only attempts to read an object from disk if the type of the
    filename argument is string.

    Parameters
    ----------
    filename: str or dict
        Path to file on disk.

    Returns
    -------
    dict
    """
    if isinstance(filename, dict):
        return filename
    return util.read_object(filename=filename)


"""List of environment variables and their default settings and an optional
value case function.
"""

ENV = [
    (FLOWSERV_BASEDIR, API_DEFAULTDIR(), None),
    (FLOWSERV_API_HOST, DEFAULT_HOST, None),
    (FLOWSERV_API_NAME, DEFAULT_NAME, None),
    (FLOWSERV_API_PATH, DEFAULT_PATH, None),
    (FLOWSERV_API_PORT, DEFAULT_PORT, to_int),
    (FLOWSERV_API_PROTOCOL, DEFAULT_PROTOCOL, None),
    (FLOWSERV_APP, None, None),
    (FLOWSERV_AUTH_LOGINTTL, DEFAULT_LOGINTTL, to_int),
    (FLOWSERV_AUTH, AUTH_DEFAULT, None),
    (FLOWSERV_BACKEND_CLASS, None, None),
    (FLOWSERV_BACKEND_MODULE, None, None),
    (FLOWSERV_POLL_INTERVAL, DEFAULT_POLL_INTERVAL, to_float),
    (FLOWSERV_ACCESS_TOKEN, None, None),
    (FLOWSERV_CLIENT, LOCAL_CLIENT, None),
    (FLOWSERV_DB, None, None),
    (FLOWSERV_WEBAPP, 'False', to_bool),
    (FLOWSERV_FILESTORE, FStore(basedir=API_DEFAULTDIR()), read_config_obj)
]


def env() -> Config:
    """Get configuration parameters from the environment."""
    config = Config()
    for var, default, cast in ENV:
        value = os.environ.get(var, default)
        if value is not None:
            if cast is not None:
                value = cast(value)
            config[var] = value
    return config
