# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper method to create instances of the flowServ application object."""

from typing import Optional

from flowserv.app.base import App


def flowapp(identifier: Optional[str] = None):
    """The local service function is a context manager for an open database
    connection that is used to instantiate the service class for the flaskflow
    API. The context manager ensures that the database conneciton in closed
    after a API request has been processed.

    Returns
    -------
    flowserv.app.base.App
    """
    from flowserv.app.database import flowdb
    return App(db=flowdb, key=identifier)
