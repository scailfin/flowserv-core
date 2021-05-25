# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Environment variables for configuring the serial workflow engine."""

from typing import Dict, Optional

from flowserv.controller.serial.engine.validate import validator

import flowserv.util as util


# Base directory to temporary run files
FLOWSERV_SERIAL_RUNSDIR = 'FLOWSERV_SERIAL_RUNSDIR'

# Workflow engine configuration.
FLOWSERV_SERIAL_ENGINECONFIG = 'FLOWSERV_SERIAL_ENGINECONFIG'


def ENGINECONFIG(env: Dict, validate: Optional[bool] = False) -> Dict:
    """Read engine configuration information from the file that is specified
    by the environment variable *FLOWSERV_SERIAL_ENGINECONFIG*.

    Returns an empty dictionary if the environment variable is not set. If the
    validate flag is True the read document will be validated against the
    configuration document schema that is defined in ``config.json``.

    Parameters
    ----------
    env: dict
        Configuration object that provides access to configuration
        parameters in the environment.
    validate: bool, default=False
        Validate the read configuration object if True.

    Returns
    -------
    dict
    """
    filename = env.get(FLOWSERV_SERIAL_ENGINECONFIG)
    if not filename:
        return dict()
    doc = util.read_object(filename=filename)
    if validate:
        validator.validate(doc)
    return doc


def RUNSDIR(env: Dict) -> str:
    """The default base directory for workflow run files.

    Parameters
    ----------
    env: dict
        Configuration object that provides access to configuration
        parameters in the environment.

    Returns
    -------
    string
    """
    return env.get(FLOWSERV_SERIAL_RUNSDIR, 'runs')
