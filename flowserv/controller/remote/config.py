# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Additional environment variables that control the configuration of the
remote workflow controller.
"""

from flowserv.config.base import get_variable


# Poll interval
FLOWSERV_POLL_INTERVAL = 'FLOWSERV_POLLINTERVAL'

# Default value for the poll interval.
DEFAULT_POLL_INTERVAL = 2


def POLL_INTERVAL(value=None):
    """Get the poll interval for the remote workflow monitor. If a value for
    the interval was given (e.g., by the user) the value will be returned.
    Otherwise, the default value (2.0) is returned.

    Parameters
    ----------
    value: float, default=None
        User-provided value.

    Returns
    -------
    float
    """
    if value is not None:
        return value
    try:
        return float(
            get_variable(
                name=FLOWSERV_POLL_INTERVAL,
                default_value=DEFAULT_POLL_INTERVAL,
                raise_error=False
            )
        )
    except ValueError:
        return DEFAULT_POLL_INTERVAL
