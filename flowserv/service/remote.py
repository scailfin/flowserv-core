# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper functions for the remote service client."""

from typing import Dict

import flowserv.config.client as config


"""Name of the header element that contains the access token."""
HEADER_TOKEN = 'api_key'


def headers() -> Dict:
    """Get dictionary of header elements for HTTP requests to a remote API.

    Returns
    -------
    dict
    """
    return {HEADER_TOKEN: config.ACCESS_TOKEN()}
