# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Additional environment variables that control the configuration of the
serial workflow controller.
"""

import os


from flowserv.config.api import API_BASEDIR
from flowserv.config.base import get_variable


# Base directory to temporary run files
FLOWSERV_RUNSDIR = 'FLOWSERV_RUNSDIR'
DEFAULT_RUNSDIR = 'runs'


def RUNSDIR() -> str:
    """Path to the subfolder in the API base directory where temporary run
    results are maintained. The result is either the value of the environment
    variable FLOWSERV_API_RUNS or the default value which is the subfolder
    'runs' under the API base directory.

    Returns
    -------
    string
    """
    return get_variable(
        name=FLOWSERV_RUNSDIR,
        default_value=os.path.join(API_BASEDIR(), DEFAULT_RUNSDIR),
        raise_error=False
    )
