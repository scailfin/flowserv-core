# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper method to create instances of the flowServ application object."""

from typing import Callable, Optional

from flowserv.app.base import App
from flowserv.controller.base import WorkflowController
from flowserv.model.auth import open_access
from flowserv.model.database import DB
from flowserv.model.files.base import FileStore

from flowserv.app.base import install_app as install  # noqa: F401
from flowserv.app.base import uninstall_app as uninstall  # noqa: F401


def open(
    identifier: Optional[str] = None, db: Optional[DB] = None,
    engine: Optional[WorkflowController] = None,
    fs: Optional[FileStore] = None, auth: Optional[Callable] = None,
):
    """Get an instance of the flowserv application for a given workflow.

    By default an open access authentication policy is used.

    Parameters
    ----------
    identifier: string, default=None
        Unique application identifier.
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    engine: flowserv.controller.base.WorkflowController, default=None
        Workflow controller to execute application runs.
    auth: callable, default=None
        Fuunction to generate an instance of the authentication policy.
        To generate an Auth instance we need a database session object.
        Thus, we cannot pass a default Auth object (for test purposes) to
        the app class.
    fs: flowserv.model.files.base.FileStore, default=None
        File store to access application files.

    Returns
    -------
    flowserv.app.base.App
    """
    # Use open access authentication by default.
    auth = auth if auth is not None else open_access
    return App(db=db, engine=engine, fs=fs, auth=auth, key=identifier)
