# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for running workflow templates."""

import os
import pytest

from flowserv.model.workflow.resource import FSObject
from flowserv.service.api import API
from flowserv.tests.controller import StateEngine
from flowserv.tests.files import FakeStream

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.workflow.state as st
import flowserv.tests.db as db
import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default users
USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()
USER_3 = util.get_unique_identifier()


def init(basedir):
    """Initialize a database with two workflows and two groups each. Returns
    the service API, the workflow controller, and a list of (workflow_id,
    group_id, user_id)-pairs.
    """
    con = db.init_db(str(basedir), users=[USER_1, USER_2, USER_3]).connect()
    engine = StateEngine()
    api = API(con=con, engine=engine, basedir=str(basedir))
    # Create two workflows with two groups each
    r = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    wf1 = r['id']
    r = api.workflows().create_workflow(name='W2', sourcedir=TEMPLATE_DIR)
    wf2 = r['id']
    # Create two groups for each workflow
    groups = list()
    r = api.groups().create_group(workflow_id=wf1, name='G1', user_id=USER_1)
    groups.append((wf1, r['id'], USER_1))
    r = api.groups().create_group(workflow_id=wf1, name='G2', user_id=USER_2)
    groups.append((wf1, r['id'], USER_2))
    r = api.groups().create_group(workflow_id=wf2, name='G1', user_id=USER_1)
    groups.append((wf2, r['id'], USER_1))
    r = api.groups().create_group(workflow_id=wf2, name='G2', user_id=USER_2)
    groups.append((wf2, r['id'], USER_2))
    return api, engine, groups


def test_multi_workflow_run(tmpdir):
    """Test creating runs for multiple workflows and groups."""
    # Initialize the database and the API
    api, engine, groups = init(tmpdir)
    # Create two successful runs for each group
    for w_id, g_id, u_id in groups:
        r = api.uploads().upload_file(
            group_id=g_id,
            file=FakeStream(data=['Alice', 'Bob'], format='txt/plain'),
            name='n.txt',
            user_id=u_id
        )
        f_id = r['id']
        for i in range(2):
            r = api.runs().start_run(
                group_id=g_id,
                arguments=[{'id': 'names', 'value': f_id}],
                user_id=u_id
            )
            r_id = r['id']
            engine.start(r_id)
            run = api.run_manager.get_run(r_id)
            fn = os.path.join(run.rundir, 'results/data.json')
            data = {'group': g_id, 'run': r_id}
            f1 = FSObject(
                identifier=util.get_unique_identifier(),
                name='results/data.json',
                filename=FakeStream(data=data).save(fn)
            )
            fn = os.path.join(run.rundir, 'values.txt')
            data = [g_id, r_id]
            f2 = FSObject(
                identifier=util.get_unique_identifier(),
                name='values.txt',
                filename=FakeStream(data=data, format='txt/plain').save(fn)
            )
            api.runs().update_run(
                run_id=r_id,
                state=engine.success(r_id, resources=[f1, f2])
            )
    # Read files and archives
    run_count = 0
    resource_count = 0
    for w_id, g_id, u_id in groups:
        r = api.runs().list_runs(group_id=g_id, user_id=u_id)
        for run in r['runs']:
            r_id = run['id']
            rh = api.runs().get_run(run_id=r_id, user_id=u_id)
            run_count += 1
            for res in rh['resources']:
                name = res['name']
                assert name in ['values.txt', 'results/data.json']
                resource_count += 1
                if name == 'results/data.json':
                    fh = api.runs().get_result_file(
                        run_id=r_id,
                        user_id=u_id,
                        resource_id=res['id']
                    )
                    doc = util.read_object(filename=fh.filename)
                    assert doc == {'group': g_id, 'run': r_id}
            # Get the archive
            api.runs().get_result_archive(run_id=r_id, user_id=u_id)
        # Delete the first run
        api.runs().delete_run(run_id=r['runs'][0]['id'], user_id=u_id)
        # Error deleting a run as a non-member
        with pytest.raises(err.UnauthorizedAccessError):
            api.runs().delete_run(run_id=r['runs'][1]['id'], user_id=USER_3)
        with pytest.raises(err.UnknownRunError):
            api.runs().delete_run(run_id=r['runs'][0]['id'], user_id=u_id)
    assert run_count == 8
    assert resource_count == 16
    run_count = 0
    resource_count = 0
    for w_id, g_id, u_id in groups:
        r = api.runs().list_runs(group_id=g_id, user_id=u_id)
        for run in r['runs']:
            r_id = run['id']
            rh = api.runs().get_run(run_id=r_id, user_id=u_id)
            run_count += 1
            for res in rh['resources']:
                resource_count += 1
    assert run_count == 4
    assert resource_count == 8


def test_workflow_run_view(tmpdir):
    """Test creating and updating the state of workflow runs."""
    # Initialize the database and the API
    api, engine, groups = init(tmpdir)
    _, g_id, u_id = groups[0]
    # Start workflow run
    r = api.uploads().upload_file(
        group_id=g_id,
        file=FakeStream(data=['Alice', 'Bob'], format='txt/plain'),
        name='n.txt',
        user_id=u_id
    )
    f_id = r['id']
    r = api.runs().start_run(
        group_id=g_id,
        arguments=[{'id': 'names', 'value': f_id}],
        user_id=u_id
    )
    serialize.validate_run_handle(r, state=st.STATE_PENDING)
    r_id = r['id']
    arg = r['arguments'][0]
    assert arg['id'] == 'names'
    assert arg['value']['file']['name'] == 'n.txt'
    assert arg['value']['target'] == 'data/names.txt'
    # Start run
    api.runs().update_run(run_id=r_id, state=engine.start(r_id))
    r = api.runs().get_run(run_id=r_id, user_id=USER_1)
    serialize.validate_run_handle(r, state=st.STATE_RUNNING)
    # Set run into error state
    api.runs().update_run(
        run_id=r_id,
        state=engine.error(r_id, messages=['There was', 'an error'])
    )
    r = api.runs().get_run(run_id=r_id, user_id=USER_1)
    serialize.validate_run_handle(r, state=st.STATE_ERROR)
    assert r['messages'] == ['There was', 'an error']
    # Error accessing run as non-member
    with pytest.raises(err.UnauthorizedAccessError):
        api.runs().get_run(run_id=r_id, user_id=USER_2)
    # Create a successful run
    r = api.runs().start_run(
        group_id=g_id,
        arguments=[{'id': 'names', 'value': f_id}],
        user_id=u_id
    )
    r_id = r['id']
    engine.start(r_id)
    run = api.run_manager.get_run(r_id)
    filename = os.path.join(run.rundir, 'results/data.json')
    f = FSObject(
        identifier=util.get_unique_identifier(),
        name='results/data.json',
        filename=FakeStream(data={'a': 1}).save(filename)
    )
    api.runs().update_run(
        run_id=r_id,
        state=engine.success(r_id, resources=[f])
    )
    r = api.runs().get_run(run_id=r_id, user_id=USER_1)
    serialize.validate_run_handle(r, state=st.STATE_SUCCESS)
    # Cancel run
    r = api.runs().start_run(
        group_id=g_id,
        arguments=[{'id': 'names', 'value': f_id}],
        user_id=u_id
    )
    r_id = r['id']
    api.runs().update_run(
        run_id=r_id,
        state=engine.start(r_id)
    )
    # Error when attempting to cancel a run as non-member
    with pytest.raises(err.UnauthorizedAccessError):
        api.runs().cancel_run(run_id=r_id, user_id=USER_2, reason='no no')
    api.runs().cancel_run(run_id=r_id, user_id=USER_1, reason='no longer needed')
    r = api.runs().get_run(run_id=r_id, user_id=USER_1)
    serialize.validate_run_handle(r, state=st.STATE_CANCELED)
    assert r['messages'] == ['no longer needed']
    # Get listing of group runs
    r = api.runs().list_runs(group_id=g_id, user_id=USER_1)
    serialize.validate_run_listing(r)
    states = list()
    for run in r['runs']:
        states.append(run['state'])
    assert len(states) == 3
    assert st.STATE_ERROR in states
    assert st.STATE_CANCELED in states
    assert st.STATE_SUCCESS in states
