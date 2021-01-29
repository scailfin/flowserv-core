# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper method to create a API generator based on the current configuration
in the environment valriables.
"""

from contextlib import contextmanager
from typing import Dict, Optional

from flowserv.config import Config
from flowserv.service.api import API, APIFactory
from flowserv.service.local import LocalAPIFactory

import flowserv.config as config


# -- API factory pattern for client applications ------------------------------

def ClientAPI(
    env: Optional[Dict] = None, basedir: Optional[str] = None,
    database: Optional[str] = None, open_access: Optional[bool] = None,
    run_async: Optional[bool] = None, docker: Optional[bool] = None,
    s3bucket: Optional[str] = None, user_id: Optional[str] = None
) -> APIFactory:
    """Create an instance of the API factory that is responsible for generating
    API instances for a flowserv client.

    The main distinction here is whether a connection is made to a local instance
    of the service or to a remote instance. This distinction is made based on
    the value of the FLOWSERV_CLIENT environment variable that takes the values
    'local' or 'remote'. The default is 'local'.

    Provides the option to alter the default settings of environment variables.

    Parameters
    ----------
    env: dict, default=None
        Dictionary with configuration parameter values.
    basedir: string, default=None
        Base directory for all workflow files. If no directory is given or
        specified in the environment a temporary directory will be created.
    database: string, default=None
        Optional database connect url.
    open_access: bool, default=None
        Use an open access policy if set to True.
    run_async: bool, default=False
        Run workflows in asynchronous mode.
    docker: bool, default=False
        Use Docker workflow engine.
    s3bucket: string, default=None
        Use the S3 bucket with the given identifier to store all workflow
        files.
    user_id: string, default=None
        Optional identifier for the authenticated API user.

    Returns
    -------
    flowserv.service.api.APIFactory
    """
    # Get the base configuration settings from the environment if not given.
    env = env if env is not None else config.env()
    if not isinstance(env, Config):
        env = Config(env)
    # Update configuration based on the given optional arguments.
    if basedir is not None:
        env.basedir(basedir)
    if database is not None:
        env.database(database)
    if open_access is not None and open_access:
        env.open_access()
    # By default, the client runs all workflows synchronously.
    if run_async is not None and run_async:
        env.run_async()
    elif env.get(config.FLOWSERV_ASYNC) is None:
        env.run_sync()
    if docker is not None and docker:
        env.docker_engine()
    if s3bucket is not None:
        env.s3(s3bucket)
    # Create local or remote API factory depending on the FLOWSERV_CLIENT value.
    client = env.get(config.FLOWSERV_CLIENT, config.LOCAL_CLIENT)
    if client == config.LOCAL_CLIENT:
        return LocalAPIFactory(env=env, user_id=user_id)
    elif client == config.REMOTE_CLIENT:
        # Not implemented yet.
        pass
    raise ValueError("inalid client type '{}'".format(client))


@contextmanager
def service() -> API:
    """Context manager that returns a service API that was instantiated from the
    current configuration settings in the environment.

    Returns
    -------
    flowserv.service.api.API
    """
    # Create the API factory from the current environment settings.
    factory = ClientAPI()
    with factory() as api:
        yield api
