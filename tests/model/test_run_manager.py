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

from flowserv.model.group import WorkflowGroupManager
from flowserv.model.parameter.base import InputFile, TemplateParameter
from flowserv.model.run import RunManager
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager

import flowserv.error as err
import flowserv.util as util
import flowserv.model.parameter.declaration as pd
import flowserv.model.parameter.value as pv
import flowserv.model.workflow.state as st
import flowserv.tests.model as model


def test_cancel_run(database, tmpdir):
    """Test setting run state to canceled."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
    # -- Test set run to error state ------------------------------------------
    with database.session() as session:
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
        state = run.state()
        runs.update_run(run_id=run_id, state=state.cancel())
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        run = runs.get_run(run_id)
        state = run.state()
        assert not state.is_active()
        assert not state.is_pending()
        assert not state.is_running()
        assert state.is_canceled()
        assert not state.is_error()
        assert not state.is_success()
        assert len(state.messages) == 1


def test_create_run_errors(database, tmpdir):
    """Test error cases for create_run parameter combinations."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
    # -- Test create_run with invalid arguments -------------------------------
    with database.session() as session:
        wfrepo = WorkflowManager(session=session, fs=fs)
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        workflow = wfrepo.get_workflow(workflow_id)
        group = groups.get_group(group_id)
        with pytest.raises(ValueError):
            runs.create_run()
        with pytest.raises(ValueError):
            runs.create_run(workflow=workflow, group=group)
        with pytest.raises(ValueError):
            runs.create_run(group=group, runs=['A'])


def test_delete_run(database, tmpdir):
    """Test deleting a run."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
    rundir = fs.run_basedir(workflow_id, group_id, run_id)
    assert os.path.isdir(rundir)
    # -- Test delete run ------------------------------------------------------
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        runs.delete_run(run_id)
        # After deleting the run the run directory no longer exists.
        assert not os.path.isdir(rundir)
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        # Error when deleting an unknown run.
        runs = RunManager(session=session, fs=fs)
        with pytest.raises(err.UnknownRunError):
            runs.delete_run(run_id)


def test_error_run(database, tmpdir):
    """Test setting run state to error."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
    # -- Test set run to error state ------------------------------------------
    with database.session() as session:
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
        state = run.state()
        runs.update_run(run_id=run_id, state=state)
        messages = ['There', 'were', 'many errors']
        runs.update_run(run_id=run_id, state=state.error(messages))
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        run = runs.get_run(run_id)
        state = run.state()
        assert not state.is_active()
        assert not state.is_pending()
        assert not state.is_running()
        assert not state.is_canceled()
        assert state.is_error()
        assert not state.is_success()
        assert state.messages == messages


def test_invalid_state_transitions(database, tmpdir):
    """Test error cases for invalid state transitions."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
    # -- Test set active run to pending ---------------------------------------
    with database.session() as session:
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
        state = run.state()
        runs.update_run(run_id=run_id, state=state.start())
        with pytest.raises(err.ConstraintViolationError):
            runs.update_run(run_id=run_id, state=st.StatePending())
    # Cancel run
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        runs.update_run(run_id=run_id, state=state.cancel())
    # -- Test cannot set run to any of the inactive states --------------------
    with database.session() as session:
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        with pytest.raises(err.ConstraintViolationError):
            runs.update_run(run_id=run_id, state=state.cancel())
        with pytest.raises(err.ConstraintViolationError):
            runs.update_run(run_id=run_id, state=state.error())
        with pytest.raises(err.ConstraintViolationError):
            runs.update_run(run_id=run_id, state=state.success())


def test_list_runs(database, tmpdir):
    """Test retrieving a list of run descriptors."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two runs: one in running state and one in error state.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        group = groups.get_group(group_id)
        # Run 1 in running state
        r = runs.create_run(group=group)
        run_1 = r.run_id
        runs.update_run(run_id=run_1, state=r.state().start())
        r = runs.create_run(group=group)
        run_2 = r.run_id
        runs.update_run(run_id=run_2, state=r.state().error())
    # -- Test get listing -----------------------------------------------------
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        run_index = dict()
        for run in runs.list_runs(group_id):
            run_index[run.run_id] = run
        assert len(run_index) == 2
        assert run_index[run_1].state().is_running()
        assert run_index[run_2].state().is_error()
    # -- Test polling runs ----------------------------------------------------
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        assert len(runs.poll_runs(group_id)) == 1
        assert len(runs.poll_runs(group_id, state=st.STATE_ERROR)) == 1
        assert len(runs.poll_runs(group_id, state=st.STATE_SUCCESS)) == 0


def test_run_parameters(database, tmpdir):
    """Test creating run with template arguments."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
    # Prepare run arguments
    filename = os.path.join(str(tmpdir), 'results.json')
    util.write_object(filename=filename, obj={'A': 1})
    arguments = pv.parse_arguments(
        arguments={
            'A': 10,
            'B': True,
            'C': InputFile(
                filename=filename,
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
    # -- Test create run with arguments ---------------------------------------
    with database.session() as session:
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(
            group=groups.get_group(group_id),
            arguments=arguments
        )
        run_id = run.run_id
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        run = runs.get_run(run_id)
        arguments = run.arguments
        assert len(arguments) == 3
        assert arguments['A'] == 10
        assert arguments['B']
        assert arguments['C']['target'] == '/dev/null'


def test_success_run(database, tmpdir):
    """Test life cycle for a successful run."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
    # -- Test set run to success state ----------------------------------------
    with database.session() as session:
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        group = groups.get_group(group_id)
        run = runs.create_run(group=group)
        run_id = run.run_id
        state = run.state().start()
        runs.update_run(run_id=run_id, state=state)
        # Set run to success state
        workflow_id = group.workflow_id
        group_id = group.group_id
        rundir = fs.run_basedir(workflow_id, group_id, run_id)
        filename = os.path.join(rundir, 'results.json')
        util.write_object(filename=filename, obj={'A': 1})
        state = state.success(files=['results.json'])
        runs.update_run(run_id=run_id, state=state)
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        run = runs.get_run(run_id)
        state = run.state()
        assert not state.is_active()
        assert not state.is_pending()
        assert not state.is_running()
        assert not state.is_canceled()
        assert not state.is_error()
        assert state.is_success()
        assert len(state.files) == 1
        filename = run.get_file(by_name='results.json').filename
        assert util.read_object(filename) == {'A': 1}
