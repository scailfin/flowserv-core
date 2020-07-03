# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow run manager."""

import os
import pytest

from flowserv.files import FileHandle, InputFile
from flowserv.model.base import User, WorkflowHandle
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.parameter.base import TemplateParameter
from flowserv.model.run import RunManager
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.resource import FSObject

import flowserv.error as err
import flowserv.util as util
import flowserv.model.parameter.declaration as pd
import flowserv.model.parameter.value as pv
import flowserv.model.workflow.state as st


def init(db, basedir):
    """Create a fresh database with one user, one workflow, and one group.
    Return a run manager instance and the respective identifiers for the
    generated workflow and group.
    """
    user_id = 'U0000'
    user = User(user_id=user_id, name=user_id, secret=user_id, active=True)
    db.session.add(user)
    # Add two dummy workflow templates.
    workflow_id = 'W0000'
    workflow = WorkflowHandle(
        workflow_id=workflow_id,
        name=workflow_id,
        workflow_spec='{}'
    )
    db.session.add(workflow)
    db.session.commit()
    group_manager = WorkflowGroupManager(
        db=db,
        fs=WorkflowFileSystem(os.path.join(str(basedir), 'workflows'))
    )
    g = group_manager.create_group(
        workflow_id=workflow_id,
        name='Group 1',
        user_id=user_id,
        parameters=dict(),
        workflow_spec=dict()
    )
    manager = RunManager(
        db=db,
        fs=WorkflowFileSystem(os.path.join(basedir, 'workflows'))
    )
    return manager, workflow_id, g.group_id


def test_error_run(database, tmpdir):
    """Test setting run states to error."""
    manager, workflow_id, group_id = init(database, tmpdir)
    # Run in state running
    r1 = manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=dict()
    )
    manager.update_run(run_id=r1.run_id, state=r1.state().start())
    r1 = manager.get_run(r1.run_id)
    # Set run in error mode
    messages = ['There', 'were', 'many errors']
    manager.update_run(run_id=r1.run_id, state=r1.state().error(messages))
    r1 = manager.get_run(r1.run_id)
    state = r1.state()
    assert not state.is_active()
    assert not state.is_pending()
    assert not state.is_running()
    assert not state.is_canceled()
    assert state.is_error()
    assert not state.is_success()
    assert state.messages == messages
    # Run in state running
    r2 = manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=dict()
    )
    manager.update_run(run_id=r2.run_id, state=r2.state().start())
    r2 = manager.get_run(r2.run_id)
    # Set run into canceled state
    manager.update_run(run_id=r2.run_id, state=r2.state().cancel())
    r2 = manager.get_run(r2.run_id)
    state = r2.state()
    assert not state.is_active()
    assert not state.is_pending()
    assert not state.is_running()
    assert state.is_canceled()
    assert not state.is_error()
    assert not state.is_success()
    assert len(state.messages) == 1
    # Delete the second run
    manager.delete_run(run_id=r2.run_id)
    with pytest.raises(err.UnknownRunError):
        manager.delete_run(run_id=r2.run_id)
    r1 = manager.get_run(r1.run_id)
    state = r1.state()
    assert state.is_error()
    assert state.messages == messages


def test_invalid_state_transitions(database, tmpdir):
    """Test error cases for invalid state transitions."""
    manager, workflow_id, group_id = init(database, tmpdir)
    # Run in running state
    r = manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=dict()
    )
    manager.update_run(run_id=r.run_id, state=r.state().start())
    r = manager.get_run(r.run_id)
    # Cannot set running state to pending
    assert r.state().is_running()
    with pytest.raises(err.ConstraintViolationError):
        manager.update_run(run_id=r.run_id, state=st.StatePending())
    # Cancel run
    manager.update_run(run_id=r.run_id, state=r.state().cancel())
    r = manager.get_run(r.run_id)
    assert r.state().is_canceled()
    # Cannot set run to any of the inactive states
    with pytest.raises(err.ConstraintViolationError):
        manager.update_run(run_id=r.run_id, state=r.state())
    with pytest.raises(err.ConstraintViolationError):
        s = st.StateError(created_at=r.state().created_at)
        manager.update_run(run_id=r.run_id, state=s)
    with pytest.raises(err.ConstraintViolationError):
        s = st.StateSuccess(
            created_at=r.state().created_at,
            started_at=r.state().started_at
        )
        manager.update_run(run_id=r.run_id, state=s)


def test_list_runs(database, tmpdir):
    """Test retrieving a list of run descriptors."""
    manager, workflow_id, group_id = init(database, tmpdir)
    # Run 1 in running state
    r1 = manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=dict()
    )
    manager.update_run(run_id=r1.run_id, state=r1.state().start())
    # Run 2 in error state
    r2 = manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=dict()
    )
    manager.update_run(run_id=r2.run_id, state=r2.state().start().error())
    # Get list of two runs. Transform listing into dictionary keyed by the
    # run indentifier
    runs = dict()
    for run in manager.list_runs(group_id):
        runs[run.run_id] = run
    assert len(runs) == 2
    assert runs[r1.run_id].state().is_running()
    assert runs[r2.run_id].state().is_error()
    # Poll runs
    assert len(manager.poll_runs(group_id)) == 1
    assert len(manager.poll_runs(group_id, state=st.STATE_ERROR)) == 1
    assert len(manager.poll_runs(group_id, state=st.STATE_SUCCESS)) == 0


def test_run_parameters(database, tmpdir):
    """Test creating run with template arguments."""
    manager, workflow_id, group_id = init(database, tmpdir)
    filename = os.path.join(str(tmpdir), 'results.json')
    util.write_object(filename=filename, obj={'A': 1})
    arguments = pv.parse_arguments(
        arguments={
            'A': 10,
            'B': True,
            'C': InputFile(
                f_handle=FileHandle(filename=filename),
                target_path='/dev/null'
            )
        },
        parameters={
            'A': TemplateParameter(
                pd.parameter_declaration('A', data_type=pd.DT_INTEGER)
            ),
            'B': TemplateParameter(
                pd.parameter_declaration('B', data_type=pd.DT_BOOL)
            ),
            'C': TemplateParameter(
                pd.parameter_declaration('C', data_type=pd.DT_FILE)
            )
        }
    )
    # Create run with arguments
    r = manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=arguments
    )
    r = manager.get_run(r.run_id)
    arguments = r.arguments
    assert len(arguments) == 3
    assert arguments['A'] == 10
    assert arguments['B']
    assert arguments['C']['target'] == '/dev/null'


def test_successful_run_lifecycle(database, tmpdir):
    """Test life cycle for a successful run."""
    manager, workflow_id, group_id = init(database, tmpdir)
    # Pending run
    r = manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=dict()
    )
    state = r.state()
    assert state.is_active()
    assert state.is_pending()
    assert not state.is_running()
    assert not state.is_canceled()
    assert not state.is_error()
    assert not state.is_success()
    r = manager.get_run(r.run_id)
    state = r.state()
    assert state.is_pending()
    # Set run to active state
    manager.update_run(run_id=r.run_id, state=state.start())
    r = manager.get_run(r.run_id)
    state = r.state()
    assert state.is_active()
    assert not state.is_pending()
    assert state.is_running()
    assert not state.is_canceled()
    assert not state.is_error()
    assert not state.is_success()
    # Set run to success state
    rundir = manager.fs.run_basedir(workflow_id, group_id, r.run_id)
    filename = os.path.join(rundir, 'results.json')
    util.write_object(filename=filename, obj={'A': 1})
    resources = [
        FSObject(identifier='0001', name='results.json', filename=filename)
    ]
    state = state.success(resources=resources)
    manager.update_run(run_id=r.run_id, state=state)
    r = manager.get_run(r.run_id)
    state = r.state()
    assert not state.is_active()
    assert not state.is_pending()
    assert not state.is_running()
    assert not state.is_canceled()
    assert not state.is_error()
    assert state.is_success()
    assert len(state.resources) == 1
    f = state.resources.get_resource(name='results.json')
    filename = manager.get_resource_file(r, f.name)
    assert util.read_object(filename) == {'A': 1}
    # Delete the run.
    manager.delete_run(run_id=r.run_id)
    assert not os.path.isfile(filename)
    assert not os.path.isdir(rundir)
    with pytest.raises(err.UnknownRunError):
        manager.delete_run(run_id=r.run_id)
