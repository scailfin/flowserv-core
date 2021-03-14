# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow run manager."""

import json
import os
import pytest
import time

from flowserv.config import Config
from flowserv.model.files.fs import FileSystemStore
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.run import RunManager
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.tests.files import MemStore
from flowserv.tests.model import success_run

import flowserv.error as err
import flowserv.util as util
import flowserv.model.workflow.state as st
import flowserv.tests.model as model


# -- Helper Methods -----------------------------------------------------------

def error_run(database, fs, messages):
    """Create a run that is in error state. Returns the identifier of the
    created workflow, group, and run.
    """
    # Setup temporary run folder.
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
        state = run.state()
        runs.update_run(run_id=run_id, state=state)
        messages = ['There', 'were', 'many errors']
        runs.update_run(run_id=run_id, state=state.error(messages))
    return workflow_id, group_id, run_id


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_cancel_run(fscls, database, tmpdir):
    """Test setting run state to canceled."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
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


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_create_run_errors(fscls, database, tmpdir):
    """Test error cases for create_run parameter combinations."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
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


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_delete_run(fscls, database, tmpdir):
    """Test deleting a run."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
    _, _, run_id, _ = success_run(database, fs, tmpdir)
    # -- Test delete run ------------------------------------------------------
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        runs.delete_run(run_id)
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        # Error when deleting an unknown run.
        runs = RunManager(session=session, fs=fs)
        with pytest.raises(err.UnknownRunError):
            runs.delete_run(run_id)


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_error_run(fscls, database, tmpdir):
    """Test setting run state to error."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
    messages = ['There', 'were', 'many errors']
    _, _, run_id = error_run(database, fs, messages)
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


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_invalid_state_transitions(fscls, database, tmpdir):
    """Test error cases for invalid state transitions."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
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
        assert runs.update_run(run_id=run_id, state=state.cancel()) is None
        with pytest.raises(err.ConstraintViolationError):
            runs.update_run(run_id=run_id, state=state.error())
        with pytest.raises(err.ConstraintViolationError):
            runs.update_run(run_id=run_id, state=state.success())


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_list_runs(fscls, database, tmpdir):
    """Test retrieving a list of run descriptors."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two runs: one in running state and one in error state.
    fs = fscls(env=Config().basedir(tmpdir))
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
        assert len(runs.list_runs(group_id)) == 2
        assert len(runs.list_runs(group_id, state=st.STATE_ERROR)) == 1
        assert len(runs.list_runs(group_id, state=st.STATE_SUCCESS)) == 0


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_obsolete_runs(fscls, database, tmpdir):
    """Test deleting runs that were created before a given date."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
    # Create two runs (one SUCCESS and one ERROR) before a timestamp t1
    _, _, run_1, _ = success_run(database, fs, tmpdir)
    _, _, run_2 = error_run(database, fs, ['There were errors'])
    time.sleep(1)
    t1 = util.utc_now()
    # Create another SUCCESS run after timestamp t1
    _, _, run_3, _ = success_run(database, fs, tmpdir)
    # -- Test delete run with state filter ------------------------------------
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        assert runs.delete_obsolete_runs(date=t1, state=st.STATE_ERROR) == 1
        # After deleting the error run the two success runs still exist.
        runs.get_run(run_id=run_1)
        with pytest.raises(err.UnknownRunError):
            runs.get_run(run_id=run_2)
        runs.get_run(run_id=run_3)
    # -- Test delete all runs prior to a given date ---------------------------
    with database.session() as session:
        runs = RunManager(session=session, fs=fs)
        assert runs.delete_obsolete_runs(date=t1) == 1
        # After deleting the run the only one success runs still exist.
        with pytest.raises(err.UnknownRunError):
            runs.get_run(run_id=run_1)
        runs.get_run(run_id=run_3)


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_run_parameters(fscls, database, tmpdir):
    """Test creating run with template arguments."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
    # Prepare run arguments
    filename = os.path.join(str(tmpdir), 'results.json')
    util.write_object(filename=filename, obj={'A': 1})
    arguments = [{'id': 'A', 'value': 10}, {'id': 'B', 'value': True}]
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
        assert run.arguments == arguments


@pytest.mark.parametrize('fscls', [FileSystemStore, MemStore])
def test_success_run(fscls, database, tmpdir):
    """Test life cycle for a successful run."""
    # -- Setup ----------------------------------------------------------------
    fs = fscls(env=Config().basedir(tmpdir))
    workflow_id, _, run_id, _ = success_run(database, fs, tmpdir)
    rundir = fs.run_basedir(workflow_id=workflow_id, run_id=run_id)
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
        assert len(state.files) == 2
        key = run.get_file(by_key='A.json').key
        f = fs.load_file(key=os.path.join(rundir, key)).open()
        assert json.load(f) == {'A': 1}
        key = run.get_file(by_key='run/results/B.json').key
        f = fs.load_file(key=os.path.join(rundir, key)).open()
        assert json.load(f) == {'B': 1}
