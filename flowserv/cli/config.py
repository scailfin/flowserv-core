# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to access configuration parameters. Following the
Twelve-Factor App methodology all configuration parameters are maintained in
environment variables.

The name of methods that provide access to values from environment variables
are in upper case to emphasize that they access configuration values that are
expected to remain constant throughout the lifespan of a running application.
"""

import os

from flowserv.config.api import API_BASEDIR
from flowserv.config.engine import FLOWSERV_ENGINE
