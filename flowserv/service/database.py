# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Global default database object."""

from typing import Dict, Optional

from flowserv.config import env, FLOWSERV_DB, FLOWSERV_WEBAPP
from flowserv.model.database import DB

import flowserv.error as err


def init_db(config: Optional[Dict] = None) -> DB:
    """Update the global database variable based on the current settings in
    the environment.

    Parameters
    ----------
    config: dict
        Configuration object that provides access to configuration parameters
        in the environment.

    Returns
    -------
    flowserv.model.database.DB
    """
    # Ensure that the configuration is set.
    config = config if config is not None else env()
    # Ensure that the databse connection Url is specified in the configuration.
    url = config.get(FLOWSERV_DB)
    if url is None:
        raise err.MissingConfigurationError(FLOWSERV_DB)
    # Create a fresh instance of the database object and assign it to the
    # global database variable.
    global database
    database = DB(connect_url=url, web_app=config.get(FLOWSERV_WEBAPP))
    # Return the fresh database object.
    return database


"""The global database object is configured based on the current environment
variables.
"""

database = init_db()
