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

import os

from contextlib import contextmanager

from flowserv.config.controller import FLOWSERV_ASYNC
from flowserv.service.local import service as local_service

import flowserv.config.client as config


@contextmanager
def service():
    """
    """
    os.environ[FLOWSERV_ASYNC] = 'False'
    with local_service(access_token=config.ACCESS_TOKEN()) as api:
        yield api
