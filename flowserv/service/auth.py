# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Get authentication policy object from environment variable settings."""

from flowserv.model.auth import Auth, DefaultAuthPolicy, OpenAccessAuth
from flowserv.model.database import SessionScope

import flowserv.config.auth as config


def get_auth(session: SessionScope) -> Auth:
    """Get instance of the authentication policy that is defined in the
    environment. If no value is defined the default policy will be used.

    Parameters
    ----------
    sessions
        Database session object.

    Returns
    -------
    flowserv.model.auth.Auth
    """
    if config.AUTH_POLICY() == config.OPEN_ACCESS:
        return OpenAccessAuth(session)
    return DefaultAuthPolicy(session)
