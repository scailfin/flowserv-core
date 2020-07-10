# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Global default database object."""

from flowserv.model.database import DB

"""The global database object is configured based on the current environment
variables.
"""

database = DB()
