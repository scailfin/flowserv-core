# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Environment variables for configuring the remote workflow engine."""

from flowserv.config import DEFAULT_POLL_INTERVAL, DEFAULT_ASYNC, FLOWSERV_POLL_INTERVAL, FLOWSERV_ASYNC  # noqa: F401

# Name of the class that implements the remote workflow controller client
FLOWSERV_REMOTE_CLIENT_CLASS = 'FLOWSERV_REMOTE_CLIENT_CLASS'
# Name of the module that contains the remote workflow controller client implementation
FLOWSERV_REMOTE_CLIENT_MODULE = 'FLOWSERV_REMOTE_CLIENT_MODULE'
