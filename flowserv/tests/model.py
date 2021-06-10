# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper method for creating database objects."""

from passlib.hash import pbkdf2_sha256
from typing import Tuple

import os

from flowserv.model.base import GroupObject, RunObject, User, WorkflowObject
from flowserv.model.database import DB
from flowserv.model.files import io_file
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.run import RunManager
from flowserv.model.template.parameter import ParameterIndex
from flowserv.volume.base import StorageVolume
from flowserv.volume.fs import FileSystemStorage

import flowserv.model.workflow.state as st
import flowserv.util as util


def create_group(session, workflow_id, users):
    """Create a new workflow group in the database. Expects a workflow
    identifier and a list of user identifier. Returns the identifier for the
    created group.

    Parameters
    ----------
    session: sqlalchemy.orm.session.Session
        Database session.
    workflow_id: string
        Unique workflow identifier.
    users: list
        List of unique user identifier.

    Returns
    -------
    string
    """
    group_id = util.get_unique_identifier()
    group = GroupObject(
        group_id=group_id,
        workflow_id=workflow_id,
        name=group_id,
        owner_id=users[0],
        parameters=ParameterIndex(),
        workflow_spec=dict()
    )
    # Add users as group members.
    for user_id in users:
        user = session.query(User).filter(User.user_id == user_id).one()
        group.members.append(user)
    session.add(group)
    return group_id


def create_run(session, workflow_id, group_id):
    """Create a new group run. Returns the run identifier.

    Parameters
    ----------
    session: sqlalchemy.orm.session.Session
        Database session.
    workflow_id: string
        Unique workflow identifier.
    group_id: string
        Unique group identifier.

    Returns
    -------
    string
    """
    run_id = util.get_unique_identifier()
    run = RunObject(
        run_id=run_id,
        workflow_id=workflow_id,
        group_id=group_id,
        state_type=st.STATE_PENDING
    )
    session.add(run)
    return run_id


def create_user(session, active=True):
    """Create a new user in the database. User identifier, name and password
    are all the same UUID. Returns the user identifier.

    Parameters
    ----------
    session: sqlalchemy.orm.session.Session
        Database session.
    active: bool, default=True
        User activation flag.

    Returns
    -------
    string
    """
    user_id = util.get_unique_identifier()
    user = User(
        user_id=user_id,
        name=user_id,
        secret=pbkdf2_sha256.hash(user_id),
        active=active
    )
    session.add(user)
    return user_id


def create_workflow(session, workflow_spec=dict(), result_schema=None):
    """Create a new workflow handle for a given workflow specification. Returns
    the workflow identifier.

    Parameters
    ----------
    session: sqlalchemy.orm.session.Session
        Database session.
    workflow_spec: dict, default=dict()
        Optional workflow specification.
    result_schema: dict, default=None
        Optional result schema.

    Returns
    -------
    string
    """
    workflow_id = util.get_unique_identifier()
    workflow = WorkflowObject(
        workflow_id=workflow_id,
        name=workflow_id,
        workflow_spec=workflow_spec,
        result_schema=result_schema
    )
    session.add(workflow)
    return workflow_id


def success_run(database: DB, fs: StorageVolume, basedir: str) -> Tuple[str, str, str, str]:
    """Create a successful run with two result files:

        - A.json
        - results/B.json

    Returns the identifier of the created workflow, group, run, and user.
    """
    # Setup temporary run folder.
    runfs = FileSystemStorage(basedir=os.path.join(basedir, 'tmprun'))
    runfs.store(file=io_file({'A': 1}), dst='A.json')
    runfs.store(file=io_file({'B': 1}), dst=util.join('results', 'B.json'))
    with database.session() as session:
        user_id = create_user(session, active=True)
        workflow_id = create_workflow(session)
        group_id = create_group(session, workflow_id, users=[user_id])
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
        state = run.state()
        runs.update_run(
            run_id=run_id,
            state=state.start().success(files=['A.json', 'results/B.json']),
            runstore=runfs
        )
    return workflow_id, group_id, run_id, user_id
