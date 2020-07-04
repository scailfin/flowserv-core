# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the manager that maintains workflow result rankings."""

import os

from flowserv.model.base import User, WorkflowHandle
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.ranking import RankingManager
from flowserv.model.run import RunManager
from flowserv.model.template.schema import (
    ResultSchema, ResultColumn, SortColumn
)
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.resource import ResourceSet, WorkflowResource

import flowserv.model.parameter.declaration as pd
import flowserv.model.workflow.state as st
import flowserv.util as util


"""Result schema for the test workflows."""
RESULT_FILE_ID = 'results.json'

SCHEMA_1 = ResultSchema(
    result_file=RESULT_FILE_ID,
    columns=[
        ResultColumn('count', 'Total Count', pd.DT_INTEGER),
        ResultColumn('avg', 'avg', pd.DT_DECIMAL),
        ResultColumn('name', 'name', pd.DT_STRING, required=False)
    ],
    order_by=[SortColumn(identifier='count')]
)

SCHEMA_2 = ResultSchema(
    result_file=RESULT_FILE_ID,
    columns=[
        ResultColumn('min', 'min', pd.DT_INTEGER, path='values/min'),
        ResultColumn('max', 'max', pd.DT_INTEGER, path='max')
    ],
    order_by=[SortColumn(identifier='min', sort_desc=False)]
)


def init(db, basedir):
    """Create a fresh database with one user, two workflows, and four groups
    for each workflow. Each group has two successful runs and one error run.
    Returns the ranking manager, run manager, and workflows with their groups
    and runs.
    """
    user_id = 'U0000'
    user = User(user_id=user_id, name=user_id, secret=user_id, active=True)
    db.session.add(user)
    # Add two workflow templates.
    workflows = list()
    for i, schema in enumerate([SCHEMA_1, SCHEMA_2]):
        w_id = 'W{}'.format(i)
        wf = WorkflowHandle(
            workflow_id=w_id,
            name=w_id,
            workflow_spec='{}',
            result_schema=schema
        )
        db.session.add(wf)
        workflows.append(wf)
    db.session.commit()
    # Add four groups for each workflow.
    fs = WorkflowFileSystem(os.path.join(str(basedir), 'workflows'))
    group_manager = WorkflowGroupManager(db=db, fs=fs)
    run_manager = RunManager(db=db, fs=fs)
    objects = list()
    for wf in workflows:
        groups = list()
        for i in range(4):
            g = group_manager.create_group(
                workflow_id=wf.workflow_id,
                name='G{}'.format(i),
                user_id=user_id,
                parameters=dict(),
                workflow_spec=dict()
            )
            runs = list()
            for j in range(3):
                r = run_manager.create_run(
                    workflow_id=wf.workflow_id,
                    group_id=g.group_id
                )
                runs.append(r)
            groups.append((g, runs))
        objects.append((wf, groups))
    return RankingManager(db=db), run_manager, objects


def run_success(run_manager, run, values):
    """Set given run into success state with the given result data."""
    rundir = run_manager.fs.run_basedir(
        workflow_id=run.workflow_id,
        group_id=run.group_id,
        run_id=run.run_id
    )
    filename = os.path.join(rundir, RESULT_FILE_ID)
    util.write_object(filename=filename, obj=values)
    ts = util.utc_now()
    run_manager.update_run(
        run_id=run.run_id,
        state=st.StateSuccess(
            created_at=ts,
            started_at=ts,
            finished_at=ts,
            resources=ResourceSet([
                WorkflowResource(
                    identifier=util.get_unique_identifier(),
                    name=RESULT_FILE_ID
                )
            ])
        )
    )


def test_empty_rankings(database, tmpdir):
    """The rankings for workflows without completed runs are empty."""
    manager, _, workflows = init(database, tmpdir)
    for wf, _ in workflows:
        assert len(manager.get_ranking(wf)) == 0


def test_multi_success_runs_for_one(database, tmpdir):
    """Test rankings for workflows where each group has multiple successful
    runs.
    """
    manager, run_manager, workflows = init(database, tmpdir)
    # Set all runs for the first workflow to success. Increase the value for
    # a counter.
    wf, groups = workflows[0]
    count = 0
    asc_order = list()
    desc_order = list()
    for g, runs in groups:
        for i, r in enumerate(runs):
            run_success(
                run_manager=run_manager,
                run=r,
                values={'count': count, 'avg': 1.0, 'name': r.run_id}
            )
            count += 1
            if i == 0:
                asc_order.append(r.run_id)
        desc_order.append(r.run_id)
    ranking = manager.get_ranking(wf)
    rank_order = [e.run_id for e in ranking]
    assert rank_order == desc_order[::-1]
    ranking = manager.get_ranking(
        wf,
        order_by=[SortColumn(identifier='count', sort_desc=False)]
    )
    rank_order = [e.run_id for e in ranking]
    assert rank_order == asc_order


def test_multi_success_runs_for_all(database, tmpdir):
    """Test rankings for workflows where each group has multiple successful
    runs.
    """
    manager, run_manager, workflows = init(database, tmpdir)
    # Set all runs for the first workflow to success. Increase the value for
    # a counter.
    wf, groups = workflows[0]
    count = 0
    count_order = list()
    for g, runs in groups:
        for r in runs:
            run_success(
                run_manager=run_manager,
                run=r,
                values={'count': count, 'avg': 1.0, 'name': r.run_id}
            )
            count_order.append(r.run_id)
            count += 1
    ranking = manager.get_ranking(wf, include_all=True)
    rank_order = [e.run_id for e in ranking]
    assert rank_order == count_order[::-1]
    ranking = manager.get_ranking(
        wf,
        order_by=[SortColumn(identifier='count', sort_desc=False)],
        include_all=True
    )
    rank_order = [e.run_id for e in ranking]
    assert rank_order == count_order
