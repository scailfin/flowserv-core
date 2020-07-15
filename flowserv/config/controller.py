# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

from flowserv.config.base import get_variable

"""Environment variables that control the configuration of the workflow
controllers.
"""
FLOWSERV_ASYNC = 'FLOWSERV_ASYNCENGINE'

# By default workfows are executed astnchronously.
DEFAULT_ASYNC = 'True'


def ENGINE_ASYNC(value=None):
    """Get the value for the asynchronous workflow execution flag. If a value
    for the flag was given (e.g., by the user) the value will be returned.
    Otherwise, the default value (True) is returned.

    Parameters
    ----------
    value: bool, default=None
        User-provided value.

    Returns
    -------
    string
    """
    if value is not None:
        return value
    flag = get_variable(
        name=FLOWSERV_ASYNC,
        default_value=DEFAULT_ASYNC,
        raise_error=False
    )
    return True if flag.lower() == 'true' else False
