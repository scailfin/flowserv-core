# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for testing group and run membership."""

import pytest

from flowserv.model.auth import DefaultAuthPolicy, OpenAccessAuth

import flowserv.error as err
import flowserv.tests.model as model


@pytest.mark.parametrize(
    'authcls,open_access',
    [(DefaultAuthPolicy, False), (OpenAccessAuth, True)]
)
def test_group_and_run_membership(database, authcls, open_access):
    """Test functionality for verifying that (a) a group or run exists, and
    (b) that a user is member of a group or run.
    """
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two active users and one group and run where only
    # user 1 is a member of.
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        user_2 = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_1])
        run_id = model.create_run(session, workflow_id, group_id)
    # -- Test group or run exists ---------------------------------------------
    with database.session() as session:
        auth = authcls(session)
        assert auth.group_or_run_exists(group_id=group_id) == group_id
        assert auth.group_or_run_exists(run_id=run_id) == group_id
        with pytest.raises(err.UnknownWorkflowGroupError):
            auth.group_or_run_exists(group_id=run_id)
        with pytest.raises(err.UnknownRunError):
            auth.group_or_run_exists(run_id=group_id)
    # -- Test group and run membership ----------------------------------------
    with database.session() as session:
        auth = authcls(session)
        assert auth.is_group_member(user_1, group_id=group_id)
        assert auth.is_group_member(user_1, run_id=run_id)
        assert auth.is_group_member(user_2, group_id=group_id) == open_access
        assert auth.is_group_member(user_2, run_id=run_id) == open_access
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        auth = authcls(session)
        with pytest.raises(ValueError):
            auth.group_or_run_exists()
        with pytest.raises(ValueError):
            auth.group_or_run_exists(group_id=group_id, run_id=run_id)
