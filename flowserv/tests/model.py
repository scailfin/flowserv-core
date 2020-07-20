# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper method for creating database objects."""

from passlib.hash import pbkdf2_sha256

from flowserv.model.base import GroupHandle, RunHandle, User, WorkflowHandle
from flowserv.model.template.parameter import ParameterIndex

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
    group = GroupHandle(
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
    run = RunHandle(
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
    workflow = WorkflowHandle(
        workflow_id=workflow_id,
        name=workflow_id,
        workflow_spec=workflow_spec,
        result_schema=result_schema
    )
    session.add(workflow)
    return workflow_id
