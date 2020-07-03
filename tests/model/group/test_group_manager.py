# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the functionality of the workflow group manager."""

import os
import pytest

from flowserv.model.base import User, WorkflowHandle
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.workflow.fs import WorkflowFileSystem

import flowserv.error as err
import flowserv.model.parameter.base as pb
import flowserv.model.parameter.declaration as pd


"""Unique identifier for users and workflow templates."""
USER_1 = '0000'
USER_2 = '0001'
USER_3 = '0002'
WORKFLOW_1 = '0004'
WORKFLOW_2 = '0005'


def init(db, basedir):
    """Create a fresh database with three users and two workflows. Returns an
    instance of the workflow group manager class.
    """
    # Register three new users.
    for user_id in [USER_1, USER_2, USER_3]:
        user = User(user_id=user_id, name=user_id, secret=user_id, active=True)
        db.session.add(user)
    # Add two dummy workflow templates.
    for workflow_id in [WORKFLOW_1, WORKFLOW_2]:
        workflow = WorkflowHandle(
            workflow_id=workflow_id,
            name=workflow_id,
            workflow_spec='{}'
        )
        db.session.add(workflow)
    db.session.commit()
    return WorkflowGroupManager(
        db=db,
        fs=WorkflowFileSystem(os.path.join(basedir, 'workflows'))
    )


def test_create_workflow_group(database, tmpdir):
    """Test creating and retrieving new workflow groups."""
    # Create database and the group manager
    manager = init(database, tmpdir)
    # Create a new workflow group with a single user
    g1 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 1',
        user_id=USER_1,
        parameters=dict(),
        workflow_spec=dict()
    )
    assert g1.name == 'Group 1'
    assert g1.owner_id == USER_1
    assert len(g1.members) == 1
    assert isinstance(g1.parameters, dict)
    assert len(g1.parameters) == 0
    assert isinstance(g1.workflow_spec, dict)
    assert len(g1.workflow_spec) == 0
    # Retrieve the group from the database
    g1 = manager.get_group(g1.group_id)
    assert g1.name == 'Group 1'
    assert g1.owner_id == USER_1
    assert len(g1.members) == 1
    assert isinstance(g1.parameters, dict)
    assert len(g1.parameters) == 0
    assert isinstance(g1.workflow_spec, dict)
    assert len(g1.workflow_spec) == 0
    # Create second group where all users are members
    workflow_spec = dict({'files': ['file1', 'file2']})
    g2 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 2',
        user_id=USER_2,
        parameters=pb.create_parameter_index([
            pd.parameter_declaration('p1'),
            pd.parameter_declaration(
                identifier='p2',
                name='A',
                data_type=pd.DT_INTEGER
            )
        ]),
        workflow_spec=workflow_spec,
        members=[USER_1, USER_3]
    )
    assert g2.owner_id == USER_2
    assert len(g2.members) == 3
    assert len(g2.parameters) == 2
    assert 'p1' in g2.parameters
    assert 'p2' in g2.parameters
    assert g2.parameters['p2'].name == 'A'
    assert g2.parameters['p2'].data_type == pd.DT_INTEGER
    assert g2.workflow_spec == workflow_spec
    for user_id in [USER_1, USER_2, USER_3]:
        assert user_id in [u.user_id for u in g2.members]
    # Retrieve the group from the database
    g2 = manager.get_group(g2.group_id)
    assert g2.owner_id == USER_2
    assert len(g2.members) == 3
    assert len(g2.parameters) == 2
    assert g2.workflow_spec == workflow_spec
    # Duplicate users in the member list
    g3 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 3',
        user_id=USER_3,
        parameters=dict(),
        workflow_spec=dict(),
        members=[USER_1, USER_3, USER_2, USER_1, USER_3]
    )
    assert len(g3.members) == 3
    for user_id in [USER_1, USER_3]:
        assert user_id in [u.user_id for u in g3.members]
    # Retrieve the group from the database
    g3 = manager.get_group(g3.group_id)
    assert len(g3.members) == 3
    for user_id in [USER_1, USER_3]:
        assert user_id in [u.user_id for u in g3.members]
    # Error conditions
    # - Invalid name
    with pytest.raises(err.ConstraintViolationError):
        manager.create_group(
            workflow_id=WORKFLOW_1,
            name='A' * 513,
            user_id=USER_1,
            parameters=dict(),
            workflow_spec=dict()
        )
    # - Duplicate name
    with pytest.raises(err.ConstraintViolationError):
        manager.create_group(
            workflow_id=WORKFLOW_1,
            name='Group 1',
            user_id=USER_1,
            parameters=dict(),
            workflow_spec=dict()
        )
    # - Unknown user
    with pytest.raises(err.UnknownUserError):
        manager.create_group(
            workflow_id=WORKFLOW_1,
            name='D',
            user_id=USER_1,
            parameters=dict(),
            workflow_spec=dict(),
            members=[USER_2, 'not a user']
        )


def test_delete_group(database, tmpdir):
    """Test creating and deleting workflow groups."""
    # Create database and the group manager
    manager = init(database, tmpdir)
    # Create two new workflow groups with a single user
    g1 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 1',
        user_id=USER_1,
        parameters=dict(),
        workflow_spec=dict()
    )
    g2 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 2',
        user_id=USER_2,
        parameters=dict(),
        workflow_spec=dict()
    )
    groupdir_1 = manager.fs.workflow_groupdir(WORKFLOW_1, g1.group_id)
    groupdir_2 = manager.fs.workflow_groupdir(WORKFLOW_1, g2.group_id)
    assert os.path.isdir(groupdir_1)
    assert os.path.isdir(groupdir_2)
    # Delete the first group. Ensure that the group folder is also removed
    manager.delete_group(g1.group_id)
    assert not os.path.isdir(groupdir_1)
    assert os.path.isdir(groupdir_2)
    # Access to the deleted group will raise an error
    with pytest.raises(err.UnknownWorkflowGroupError):
        manager.get_group(g1.group_id)
    with pytest.raises(err.UnknownWorkflowGroupError):
        manager.delete_group(g1.group_id)
    # Delete the second group
    manager.get_group(g2.group_id)
    manager.delete_group(g2.group_id)
    assert not os.path.isdir(groupdir_2)
    with pytest.raises(err.UnknownWorkflowGroupError):
        manager.get_group(g1.group_id)


def test_list_groups(database, tmpdir):
    """Test listing groups by user or by workflow."""
    # Create database and the group manager
    manager = init(database, tmpdir)
    # Create three new groups, two for the first workflow and one for the
    # second workflow
    manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 1',
        user_id=USER_1,
        parameters=dict(),
        workflow_spec=dict()
    )
    manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 2',
        user_id=USER_2,
        members=[USER_3],
        parameters=dict(),
        workflow_spec=dict()
    )
    manager.create_group(
        workflow_id=WORKFLOW_2,
        name='Group 3',
        user_id=USER_3,
        members=[USER_1],
        parameters=dict(),
        workflow_spec=dict()
    )
    # Get names of all groups in the database
    validate_groups(
        groups=manager.list_groups(),
        names=['Group 1', 'Group 2', 'Group 3']
    )
    # Workflow 1 has two groups and workflow 2 has one group
    validate_groups(
        groups=manager.list_groups(workflow_id=WORKFLOW_1),
        names=['Group 1', 'Group 2']
    )
    validate_groups(
        groups=manager.list_groups(workflow_id=WORKFLOW_2),
        names=['Group 3']
    )
    # User 1 and user 3 are member of two groups. User 2 is member of one group
    validate_groups(
        groups=manager.list_groups(user_id=USER_1),
        names=['Group 1', 'Group 3']
    )
    validate_groups(
        groups=manager.list_groups(user_id=USER_2),
        names=['Group 2']
    )
    validate_groups(
        groups=manager.list_groups(user_id=USER_3),
        names=['Group 2', 'Group 3']
    )
    # List submissions with both optional parameters given
    validate_groups(
        groups=manager.list_groups(user_id=USER_1, workflow_id=WORKFLOW_1),
        names=['Group 1']
    )


def test_update_groups(database, tmpdir):
    """Test updating group name and group members."""
    # Create database and the group manager
    manager = init(database, tmpdir)
    # Create three new groups, two for the first workflow and one for the
    # second workflow
    g1 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 1',
        user_id=USER_1,
        parameters=dict(),
        workflow_spec=dict()
    )
    g2 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 2',
        user_id=USER_2,
        members=[USER_3],
        parameters=dict(),
        workflow_spec=dict()
    )
    # Change add USER_3 to group 1.
    manager.update_group(g1.group_id, members=[USER_1, USER_3])
    # Rename group 2 and replace USER_3 with USER_1.
    manager.update_group(
        g2.group_id,
        name='My Group',
        members=[USER_1, USER_2]
    )
    g1 = manager.get_group(g1.group_id)
    assert g1.name == 'Group 1'
    assert {USER_1, USER_3} == set([u.user_id for u in g1.members])
    g2 = manager.get_group(g2.group_id)
    assert g2.name == 'My Group'
    assert {USER_1, USER_2} == set([u.user_id for u in g2.members])
    # No changes
    manager.update_group(g1.group_id)
    g1 = manager.get_group(g1.group_id)
    assert g1.name == 'Group 1'
    assert {USER_1, USER_3} == set([u.user_id for u in g1.members])
    manager.update_group(g1.group_id, name=g1.name)
    g1 = manager.get_group(g1.group_id)
    assert g1.name == 'Group 1'
    assert {USER_1, USER_3} == set([u.user_id for u in g1.members])


def validate_groups(groups, names):
    """Ensure that the given names exactly match the names of the descriptors
    in the group listing.
    """
    group_names = [g.name for g in groups]
    assert len(groups) == len(names)
    for name in names:
        assert name in group_names
