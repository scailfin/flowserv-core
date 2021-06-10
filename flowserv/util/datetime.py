# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Utility methods for date time conversion."""

import datetime

from dateutil.parser import isoparse
from dateutil.tz import UTC


def to_datetime(timestamp: str) -> datetime.datetime:
    """Converts a timestamp string in ISO format into a datatime object.

    Parameters
    ----------
    timstamp : string
        Timestamp in ISO format

    Returns
    -------
    datetime.datetime
        Datetime object
    """
    # Assumes a string in ISO format (with or without milliseconds)
    for format in ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S']:
        try:
            return datetime.datetime.strptime(timestamp, format)
        except ValueError:
            pass
    return isoparse(timestamp)


def utc_now() -> str:
    """Get the current time in UTC timezone as a string in ISO format.

    Returns
    -------
    string
    """
    return datetime.datetime.now(UTC).isoformat()
