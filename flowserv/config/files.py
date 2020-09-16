# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module defines environment variables that are used to configure the
file store that is used to maintainf files for workflow templates, user group
uploads, and workflow runs. Different file store implementations may define
additional environment variables for their configuration.
"""

from flowserv.config.api import FLOWSERV_API_BASEDIR  # noqa: F401


# Name of the class that implements the file store interface
FLOWSERV_FILESTORE_CLASS = 'FLOWSERV_FILESTORE_CLASS'
# Name of the module that contains the file store implementation
FLOWSERV_FILESTORE_MODULE = 'FLOWSERV_FILESTORE_MODULE'
