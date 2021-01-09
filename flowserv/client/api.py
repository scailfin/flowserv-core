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

from typing import Dict, Optional

from flowserv.service.api import APIFactory


# -- API factory pattern for client applications ------------------------------

def service(env: Optional[Dict] = None) -> APIFactory:
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
    flowserv.service.api.APIFactory
    """
    pass
