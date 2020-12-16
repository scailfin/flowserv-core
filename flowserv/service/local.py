# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes method to create instances of the API components. All
components use the same underlying database connection. The connection object
is under the control of of a context manager to ensure that the connection is
closed properly after every API request has been handled.
"""

from contextlib import contextmanager
from typing import Optional

from flowserv.controller.base import WorkflowController
from flowserv.model.auth import Auth
from flowserv.model.database import DB, SessionScope
from flowserv.model.files.base import FileStore
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.ranking import RankingManager
from flowserv.model.run import RunManager
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.service.api import API
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.files.base import get_filestore
from flowserv.service.files.local import LocalUploadFileService
from flowserv.service.group.local import LocalWorkflowGroupService
from flowserv.service.run.local import LocalRunService
from flowserv.service.user.local import LocalUserService
from flowserv.service.workflow.local import LocalWorkflowService

from flowserv.model.user import UserManager
from flowserv.service.auth import get_auth


@contextmanager
def service(
    db: Optional[DB] = None, engine: Optional[WorkflowController] = None,
    fs: Optional[FileStore] = None, auth: Optional[Auth] = None,
    user_id: Optional[str] = None
) -> API:
    """The local service function is a context manager for an open database
    connection that is used to instantiate the API service class. The context
    manager ensures that the database connection is closed after an API request
    has been processed.

    Parameters
    ----------
    db: flowserv.model.database.DB, default=None
        Database manager.
    engine: flowserv.controller.base.WorkflowController, default=None
        Workflow controller used by the API for workflow execution
    fs: flowserv.model.files.base.FileStore, default=None
        File store for accessing and maintaining files for workflows,
        groups and workflow runs.
    auth: flowserv.model.user.auth.Auth, default=None
        Authentication and authorization policy
    user_id: string, default=None
        Optional identifier of a user that has been authenticated.

    Returns
    -------
    flowserv.service.api.API
    """
    # Ensure that the database is not None.
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    # Ensure that the engine is not None.
    if engine is None:
        # Use the global backend if no engine is specified
        from flowserv.service.backend import backend
        engine = backend
    # Initialize the file store.
    if fs is None:
        fs = get_filestore()
    with db.session() as session:
        yield create_api(
            session=session,
            engine=engine,
            fs=fs,
            auth=auth,
            user_id=user_id
        )


# -- Helper Methods -----------------------------------------------------------

def create_api(
    session: SessionScope, engine: WorkflowController, fs: FileStore,
    auth: Auth, user_id: str
):
    """Helper method to create an instance of the local service API.


    Parameters
    ----------
    session: flowserv.model.database.SessionScope
        Open database session.
    engine: flowserv.controller.base.WorkflowController
        Workflow controller used by the API for workflow execution.
    fs: flowserv.model.files.base.FileStore
        File store for accessing and maintaining files for workflows,
        groups and workflow runs.
    auth: flowserv.model.user.auth.Auth
        Authentication and authorization policy.
    user_id: string, default=None
        Optional identifier of a user that has been authenticated.

    Returns
    -------
    flowserv.service.api.API
    """
    if auth is None:
        auth = get_auth(session)
    user_manager = UserManager(session=session)
    group_manager = WorkflowGroupManager(
        session=session,
        fs=fs,
        users=user_manager
    )
    ranking_manager = RankingManager(session=session)
    run_manager = RunManager(session=session, fs=fs)
    workflow_repo = WorkflowManager(session=session, fs=fs)
    return API(
        service=ServiceDescriptor(),
        workflow_service=LocalWorkflowService(
            workflow_repo=workflow_repo,
            ranking_manager=ranking_manager,
            run_manager=run_manager
        ),
        group_service=LocalWorkflowGroupService(
            group_manager=group_manager,
            workflow_repo=workflow_repo,
            backend=engine,
            auth=auth,
            user_id=user_id
        ),
        upload_service=LocalUploadFileService(
            group_manager=group_manager,
            auth=auth,
            user_id=user_id
        ),
        run_service=LocalRunService(
            run_manager=run_manager,
            group_manager=group_manager,
            ranking_manager=ranking_manager,
            backend=engine,
            auth=auth,
            user_id=user_id
        ),
        user_service=LocalUserService(
            manager=user_manager,
            auth=auth
        )
    )
