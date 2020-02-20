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

from flowserv.core.files import FileHandle, InputFile
from flowserv.model.group.manager import WorkflowGroupManager
from flowserv.model.parameter.base import TemplateParameter
from flowserv.model.run.manager import RunManager
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.resource import FSObject

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.parameter.declaration as pd
import flowserv.model.parameter.value as pv
import flowserv.model.workflow.state as st
import flowserv.tests.db as db


"""Unique identifier for user and workflow."""
USER_1 = '0000'
WORKFLOW_1 = '0000'


def init(basedir):
    """Create a fresh database with one user, one workflow, and one group.
    Return a run manager instance and the handle for the generated group.
    """
    # Create new database with three users
    connector = db.init_db(
        str(basedir),
        workflows=[WORKFLOW_1],
        users=[USER_1]
    )
    con = connector.connect()
    group_manager = WorkflowGroupManager(
        con=con,
        fs=WorkflowFileSystem(os.path.join(str(basedir), 'workflows'))
    )
    g = group_manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 1',
        user_id=USER_1,
        parameters=dict(),
        workflow_spec=dict()
    )
    manager = RunManager(
        con=con,
        fs=WorkflowFileSystem(os.path.join(str(basedir), 'workflows'))
    )
    return manager, g


def test_error_run(tmpdir):
    """Test setting run states to error."""
    manager, g = init(tmpdir)
    # Run in state running
    r1 = manager.create_run(
        workflow_id=WORKFLOW_1,
        group_id=g.identifier,
        arguments=dict()
    )
    manager.update_run(run_id=r1.identifier, state=r1.state.start())
    r1 = manager.get_run(r1.identifier)
    # Set run in error mode
    messages = ['There', 'were', 'many errors']
    manager.update_run(run_id=r1.identifier, state=r1.state.error(messages))
    r1 = manager.get_run(r1.identifier)
    assert not r1.is_active()
    assert not r1.is_pending()
    assert not r1.is_running()
    assert not r1.is_canceled()
    assert r1.is_error()
    assert not r1.is_success()
    assert len(r1.resources) == 0
    assert r1.messages == messages
    # Run in state running
    r2 = manager.create_run(
        workflow_id=WORKFLOW_1,
        group_id=g.identifier,
        arguments=dict()
    )
    manager.update_run(run_id=r2.identifier, state=r2.state.start())
    r2 = manager.get_run(r2.identifier)
    # Set run into canceled state
    manager.update_run(run_id=r2.identifier, state=r2.state.cancel())
    r2 = manager.get_run(r2.identifier)
    assert not r2.is_active()
    assert not r2.is_pending()
    assert not r2.is_running()
    assert r2.is_canceled()
    assert not r2.is_error()
    assert not r2.is_success()
    assert len(r2.resources) == 0
    assert len(r2.messages) == 1
    # Delete the second run
    manager.delete_run(run_id=r2.identifier)
    with pytest.raises(err.UnknownRunError):
        manager.delete_run(run_id=r2.identifier)
    r1 = manager.get_run(r1.identifier)
    assert r1.is_error()
    assert r1.messages == messages


def test_invalid_state_transitions(tmpdir):
    """Test error cases for invalid state transitions."""
    manager, g = init(tmpdir)
    # Run in running state
    r = manager.create_run(
        workflow_id=WORKFLOW_1,
        group_id=g.identifier,
        arguments=dict()
    )
    manager.update_run(run_id=r.identifier, state=r.state.start())
    r = manager.get_run(r.identifier)
    # Cannot set running state to pending
    assert r.is_running()
    with pytest.raises(err.ConstraintViolationError):
        manager.update_run(run_id=r.identifier, state=st.StatePending())
    # Cancel run
    manager.update_run(run_id=r.identifier, state=r.state.cancel())
    r = manager.get_run(r.identifier)
    assert r.is_canceled()
    # Cannot set run to any of the inactive states
    with pytest.raises(err.ConstraintViolationError):
        manager.update_run(run_id=r.identifier, state=r.state)
    with pytest.raises(err.ConstraintViolationError):
        s = st.StateError(created_at=r.state.created_at)
        manager.update_run(run_id=r.identifier, state=s)
    with pytest.raises(err.ConstraintViolationError):
        s = st.StateSuccess(
            created_at=r.state.created_at,
            started_at=r.state.started_at
        )
        manager.update_run(run_id=r.identifier, state=s)


def test_list_runs(tmpdir):
    """Test retrieving a list of run descriptors."""
    manager, g = init(tmpdir)
    # Run 1 in running state
    r1 = manager.create_run(
        workflow_id=WORKFLOW_1,
        group_id=g.identifier,
        arguments=dict()
    )
    manager.update_run(run_id=r1.identifier, state=r1.state.start())
    # Run 2 in error state
    r2 = manager.create_run(
        workflow_id=WORKFLOW_1,
        group_id=g.identifier,
        arguments=dict()
    )
    manager.update_run(run_id=r2.identifier, state=r2.state.start().error())
    # Get list of two runs. Transform listing into dictionary keyed by the
    # run indentifier
    runs = dict()
    for run in manager.list_runs(g.identifier):
        runs[run.identifier] = run
    assert len(runs) == 2
    assert runs[r1.identifier].is_running()
    assert runs[r2.identifier].is_error()
    # Poll runs
    assert len(manager.poll_runs(g.identifier)) == 1
    assert len(manager.poll_runs(g.identifier, state=st.STATE_ERROR)) == 1
    assert len(manager.poll_runs(g.identifier, state=st.STATE_SUCCESS)) == 0


def test_run_parameters(tmpdir):
    """Test creating run with template arguments."""
    manager, g = init(tmpdir)
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
        workflow_id=WORKFLOW_1,
        group_id=g.identifier,
        arguments=arguments
    )
    r = manager.get_run(r.identifier)
    arguments = r.arguments
    assert len(arguments) == 3
    assert arguments['A'] == 10
    assert arguments['B']
    assert arguments['C']['target'] == '/dev/null'


def test_successful_run_lifecycle(tmpdir):
    """Test life cycle for a successful run."""
    manager, g = init(tmpdir)
    # Pending run
    r = manager.create_run(
        workflow_id=WORKFLOW_1,
        group_id=g.identifier,
        arguments=dict()
    )
    assert r.is_active()
    assert r.is_pending()
    assert not r.is_running()
    assert not r.is_canceled()
    assert not r.is_error()
    assert not r.is_success()
    assert len(r.resources) == 0
    r = manager.get_run(r.identifier)
    assert r.is_pending()
    # Set run to active state
    manager.update_run(run_id=r.identifier, state=r.state.start())
    r = manager.get_run(r.identifier)
    assert r.is_active()
    assert not r.is_pending()
    assert r.is_running()
    assert not r.is_canceled()
    assert not r.is_error()
    assert not r.is_success()
    assert len(r.resources) == 0
    # Set run to success state
    rundir = manager.fs.run_basedir(WORKFLOW_1, g.identifier, r.identifier)
    filename = os.path.join(rundir, 'results.json')
    util.write_object(filename=filename, obj={'A': 1})
    resources = [
        FSObject(identifier='0001', name='results.json', filename=filename)
    ]
    state = r.state.success(resources=resources)
    manager.update_run(run_id=r.identifier, state=state)
    r = manager.get_run(r.identifier)
    assert not r.is_active()
    assert not r.is_pending()
    assert not r.is_running()
    assert not r.is_canceled()
    assert not r.is_error()
    assert r.is_success()
    assert len(r.resources) == 1
    f = r.resources.get_resource(name='results.json')
    assert util.read_object(f.filename) == {'A': 1}
    # Delete the run.
    manager.delete_run(run_id=r.identifier)
    assert not os.path.isfile(f.filename)
    assert not os.path.isdir(rundir)
    with pytest.raises(err.UnknownRunError):
        manager.delete_run(run_id=r.identifier)
