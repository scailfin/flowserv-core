# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the functionality of the workflow group manager."""

import pytest

from flowserv.model.files.fs import FileSystemStore
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.template.parameter import ParameterIndex
from flowserv.tests.files import DiskStore

import flowserv.error as err
import flowserv.tests.model as model


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_create_group(fscls, database, tmpdir):
    """Test creating and retrieving new workflow groups."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with a single workflow.
    fs = fscls(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
    # -- Test create group ----------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        group = manager.create_group(
            workflow_id=workflow_id,
            name='Group 1',
            user_id=user_id,
            parameters=ParameterIndex(),
            workflow_spec=dict()
        )
        assert group.name == 'Group 1'
        assert group.owner_id == user_id
        assert len(group.members) == 1
        assert isinstance(group.parameters, dict)
        assert len(group.parameters) == 0
        assert isinstance(group.workflow_spec, dict)
        assert len(group.workflow_spec) == 0
        # Retrieve the group from the database
        group = manager.get_group(group.group_id)
        assert group.name == 'Group 1'
        assert group.owner_id == user_id
        assert len(group.members) == 1
        assert isinstance(group.parameters, dict)
        assert len(group.parameters) == 0
        assert isinstance(group.workflow_spec, dict)
        assert len(group.workflow_spec) == 0
    # -- Test create group with duplicate members -----------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        group = manager.create_group(
            workflow_id=workflow_id,
            name='Group 2',
            user_id=user_id,
            parameters=ParameterIndex(),
            workflow_spec=dict(),
            members=[user_id, user_id, user_id]
        )
        assert len(group.members) == 1
        # Retrieve the group from the database
        group = manager.get_group(group.group_id)
        assert len(group.members) == 1
    # -- Test error cases -----------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        # - Invalid name
        with pytest.raises(err.ConstraintViolationError):
            manager.create_group(
                workflow_id=workflow_id,
                name='A' * 513,
                user_id=user_id,
                parameters=ParameterIndex(),
                workflow_spec=dict()
            )
        # - Duplicate name
        with pytest.raises(err.ConstraintViolationError):
            manager.create_group(
                workflow_id=workflow_id,
                name='Group 1',
                user_id=user_id,
                parameters=ParameterIndex(),
                workflow_spec=dict()
            )
        # - Unknown user
        with pytest.raises(err.UnknownUserError):
            manager.create_group(
                workflow_id=workflow_id,
                name='D',
                user_id=user_id,
                parameters=ParameterIndex(),
                workflow_spec=dict(),
                members=[user_id, 'not a user']
            )


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_delete_group(fscls, database, tmpdir):
    """Test creating and deleting workflow groups."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow.
    fs = fscls(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        wf_id = model.create_workflow(session)
        manager = WorkflowGroupManager(session=session, fs=fs)
        group_1 = manager.create_group(
            workflow_id=wf_id,
            name='A',
            user_id=user_id,
            parameters=ParameterIndex(),
            workflow_spec=dict()
        ).group_id
        group_2 = manager.create_group(
            workflow_id=wf_id,
            name='B',
            user_id=user_id,
            parameters=ParameterIndex(),
            workflow_spec=dict()
        ).group_id
    # -- Delete group ---------------------------------------------------------
    with database.session() as session:
        # Ensure that group directores are deleted.
        manager = WorkflowGroupManager(session=session, fs=fs)
        manager.delete_group(group_1)
        # Access to group 1 raises error while group 2 is still accessible.
        with pytest.raises(err.UnknownWorkflowGroupError):
            manager.get_group(group_1)
        assert manager.get_group(group_2) is not None


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_list_groups(fscls, database, tmpdir):
    """Test listing groups by user or by workflow."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with three groups for a two workflow. Group 1 has
    # user 1 as only member, group 2 has user 2 and 3 as member, group 3 has
    # user 1 and 3 as members.
    fs = fscls(tmpdir)
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        user_2 = model.create_user(session, active=True)
        user_3 = model.create_user(session, active=True)
        workflow_1 = model.create_workflow(session)
        workflow_2 = model.create_workflow(session)
        members_1 = [user_1]
        group_1 = model.create_group(session, workflow_1, users=members_1)
        members_2 = [user_2, user_3]
        group_2 = model.create_group(session, workflow_1, users=members_2)
        members_3 = [user_1, user_3]
        group_3 = model.create_group(session, workflow_2, users=members_3)
    # -- Test list all groups -------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        assert len(manager.list_groups()) == 3
        assert len(manager.list_groups(workflow_1)) == 2
        assert len(manager.list_groups(workflow_2)) == 1
    # -- Test list groups for users -------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        # User 1 is member of group 1 and 3.
        groups = manager.list_groups(user_id=user_1)
        assert len(groups) == 2
        assert [g.name for g in groups] == [group_1, group_3]
        # User 2 is member of group 2.
        groups = manager.list_groups(user_id=user_2)
        assert len(groups) == 1
        assert [g.name for g in groups] == [group_2]
        # User 3 is member of group 2 and 3.
        groups = manager.list_groups(user_id=user_3)
        assert len(groups) == 2
        assert [g.name for g in groups] == [group_2, group_3]


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_update_groups(fscls, database, tmpdir):
    """Test updating group name and group members."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for one workflow. Group 1 has user 1 as
    # only member, group 2 has user 2 and 3 as member.
    fs = fscls(tmpdir)
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        user_2 = model.create_user(session, active=True)
        user_3 = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        members_1 = [user_1]
        group_1 = model.create_group(session, workflow_id, users=members_1)
        members_2 = [user_2, user_3]
        group_2 = model.create_group(session, workflow_id, users=members_2)
    # -- Test add member ------------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        members_1 = [user_1, user_3]
        manager.update_group(group_1, members=members_1)
        members = [m.user_id for m in manager.get_group(group_1).members]
        assert set(members) == set(members_1)
        members = [m.user_id for m in manager.get_group(group_2).members]
        assert set(members) == set(members_2)
    # -- Test rename group ----------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        manager.update_group(group_2, name='My Group')
        assert manager.get_group(group_1).name == group_1
        assert manager.get_group(group_2).name == 'My Group'
    # -- Test rename group and change members ---------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        members_2 = [user_1] + members_2
        manager.update_group(group_2, name='The Group', members=members_2)
        members = [m.user_id for m in manager.get_group(group_1).members]
        assert set(members) == set(members_1)
        members = [m.user_id for m in manager.get_group(group_2).members]
        assert set(members) == set(members_2)
        assert manager.get_group(group_1).name == group_1
        assert manager.get_group(group_2).name == 'The Group'
    # -- Test no changes ------------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        manager.update_group(group_2, name='The Group', members=members_2)
        members = [m.user_id for m in manager.get_group(group_1).members]
        assert set(members) == set(members_1)
        members = [m.user_id for m in manager.get_group(group_2).members]
        assert set(members) == set(members_2)
        assert manager.get_group(group_1).name == group_1
        assert manager.get_group(group_2).name == 'The Group'
