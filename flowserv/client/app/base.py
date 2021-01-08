# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

from typing import Callable, Optional

from flowserv.controller.base import WorkflowController
from flowserv.model.auth import open_access
from flowserv.model.database import DB
from flowserv.model.files.base import FileStore
from flowserv.model.workflow.manager import WorkflowManager


def install_app(
    source: str, identifier: Optional[str] = None, name: Optional[str] = None,
    description: Optional[str] = None, instructions: Optional[str] = None,
    specfile: Optional[str] = None, manifestfile: Optional[str] = None,
    ignore_postproc: Optional[bool] = False, db: Optional[DB] = None,
    fs: Optional[FileStore] = None
):
    """Create database objects for a application that is defined by a workflow
    template. An application is simply a different representation for the
    workflow that defines the application.

    Parameters
    ----------
    source: string
        Path to local template, name or URL of the template in the repository.
    identifier: string, default=None
        Unique user-provided workflow identifier. If no identifier is given a
        unique identifier will be generated for the application.
    name: string, default=None
        Unique workflow name
    description: string, default=None
        Optional short description for display in workflow listings
    instructions: string, default=None
        File containing instructions for workflow users.
    specfile: string, default=None
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)
    manifestfile: string, default=None
        Path to manifest file. If not given an attempt is made to read one
        of the default manifest file names in the base directory.
    ignore_postproc: bool, default=False
        Ignore post-processing workflow specification if True.
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    fs: flowserv.model.files.base.FileStore, default=None
        File store to access application files.

    Returns
    -------
    string
    """
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    if fs is None:
        fs = get_filestore()
    # Create a new workflow for the application from the specified template.
    with db.session() as session:
        repo = WorkflowManager(session=session, fs=fs)
        workflow = repo.create_workflow(
            source=source,
            identifier=identifier,
            name=name,
            description=description,
            instructions=instructions,
            specfile=specfile,
            manifestfile=manifestfile,
            ignore_postproc=ignore_postproc
        )
        workflow_id = workflow.workflow_id
    return workflow_id


def open_app(
    identifier: Optional[str] = None, db: Optional[DB] = None,
    engine: Optional[WorkflowController] = None,
    fs: Optional[FileStore] = None, auth: Optional[Callable] = None,
) -> App:
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


def uninstall_app(
    app_key: str, db: Optional[DB] = None, fs: Optional[FileStore] = None
):
    """Remove the workflow that is associated with the given application key.

    Parameters
    ----------
    app_key: string
        Application identifier (i.e., group identifier).
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    fs: flowserv.model.files.base.FileStore, default=None
        File store to access application files.
    """
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    if fs is None:
        fs = get_filestore()
    # Delete workflow and all related files.
    with db.session() as session:
        # Delete workflow using the workflow manager.
        WorkflowManager(session=session, fs=fs).delete_workflow(app_key)
        session.commit()
