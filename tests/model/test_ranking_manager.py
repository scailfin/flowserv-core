# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the manager that maintains workflow result rankings."""

import os

from datetime import timedelta

from flowserv.model.files import io_file
from flowserv.model.parameter.numeric import PARA_FLOAT, PARA_INT
from flowserv.model.parameter.string import PARA_STRING
from flowserv.model.ranking import RankingManager
from flowserv.model.run import RunManager
from flowserv.model.template.schema import ResultSchema, ResultColumn, SortColumn
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.volume.base import StorageFolder
from flowserv.volume.fs import FileSystemStorage

import flowserv.model.workflow.state as st
import flowserv.util as util
import flowserv.tests.model as model


"""Result schema for the test workflows."""
RESULT_FILE_ID = 'results.json'

SCHEMA_1 = ResultSchema(
    result_file=RESULT_FILE_ID,
    columns=[
        ResultColumn('count', 'Total Count', PARA_INT),
        ResultColumn('avg', 'avg', PARA_FLOAT),
        ResultColumn('name', 'name', PARA_STRING, required=False)
    ],
    order_by=[SortColumn(column_id='count')]
)

SCHEMA_2 = ResultSchema(
    result_file=RESULT_FILE_ID,
    columns=[
        ResultColumn('min', 'min', PARA_INT, path='values/min'),
        ResultColumn('max', 'max', PARA_INT, path='max')
    ],
    order_by=[SortColumn(column_id='min', sort_desc=False)]
)


def init(database, basedir):
    """Create a fresh database with one user, two workflows, and four groups
    for each workflow. Each group has three active runs. Returns a a list of
    tuples with workflow_id, groups, and runs.
    """
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        # Add two workflow templates.
        workflows = list()
        for i, schema in enumerate([SCHEMA_1, SCHEMA_2]):
            workflow_id = model.create_workflow(session, result_schema=schema)
            workflows.append(workflow_id)
        objects = list()
        for workflow_id in workflows:
            groups = list()
            for i in range(4):
                group_id = model.create_group(
                    session,
                    workflow_id,
                    users=[user_id]
                )
                runs = list()
                for j in range(3):
                    run_id = model.create_run(session, workflow_id, group_id)
                    runs.append(run_id)
                groups.append((group_id, runs))
            objects.append((workflow_id, groups))
        return objects


def run_success(run_manager, run_id, store, values):
    """Set given run into success state with the given result data."""
    store.store(file=io_file(values), dst=RESULT_FILE_ID)
    ts = util.utc_now()
    run_manager.update_run(
        run_id=run_id,
        state=st.StateSuccess(
            created_at=ts,
            started_at=ts,
            finished_at=ts,
            files=[RESULT_FILE_ID]
        ),
        runstore=store
    )


def test_empty_ranking(database, tmpdir):
    """The rankings for workflows without completed runs are empty."""
    # -- Setup ----------------------------------------------------------------
    workflows = init(database, tmpdir)
    fs = FileSystemStorage(basedir=tmpdir)
    # -- Test empty listing with no successful runs ---------------------------
    with database.session() as session:
        wfrepo = WorkflowManager(session=session, fs=fs)
        rankings = RankingManager(session=session)
        for workflow_id, _ in workflows:
            wf = wfrepo.get_workflow(workflow_id)
            assert len(rankings.get_ranking(wf)) == 0


def test_multi_success_runs(database, tmpdir):
    """Test rankings for workflows where each group has multiple successful
    runs.
    """
    # -- Setup ----------------------------------------------------------------
    # Create database with two workflows and four grous each. Each group has
    # three active runs. Then set all runs for the first workflow into success
    # state. Increase a counter for the avg_len value as we update runs.
    workflows = init(database, tmpdir)
    fs = FileSystemStorage(basedir=tmpdir)
    workflow_id, groups = workflows[0]
    count = 0
    asc_order = list()
    count_order = list()
    desc_order = list()
    with database.session() as session:
        for group_id, runs in groups:
            for i, run_id in enumerate(runs):
                tmprundir = os.path.join(tmpdir, 'runs', run_id)
                run_success(
                    run_manager=RunManager(session=session, fs=fs),
                    run_id=run_id,
                    store=StorageFolder(basedir=tmprundir, volume=fs),
                    values={'count': count, 'avg': 1.0, 'name': run_id}
                )
                count += 1
                if i == 0:
                    asc_order.append(run_id)
                count_order.append(run_id)
            desc_order.append(run_id)
    # -- Test get ranking with one result per group ---------------------------
    with database.session() as session:
        wfrepo = WorkflowManager(session=session, fs=fs)
        rankings = RankingManager(session=session)
        wf = wfrepo.get_workflow(workflow_id)
        ranking = rankings.get_ranking(wf)
        rank_order = [e.run_id for e in ranking]
        assert rank_order == desc_order[::-1]
        ranking = rankings.get_ranking(
            wf,
            order_by=[SortColumn(column_id='count', sort_desc=False)]
        )
        rank_order = [e.run_id for e in ranking]
        assert rank_order == asc_order
        # Run execution time
        assert type(ranking[0].exectime()) == timedelta
    # -- Test get ranking with all results per group --------------------------
    with database.session() as session:
        wfrepo = WorkflowManager(session=session, fs=fs)
        rankings = RankingManager(session=session)
        wf = wfrepo.get_workflow(workflow_id)
        ranking = rankings.get_ranking(wf, include_all=True)
        rank_order = [e.run_id for e in ranking]
        assert rank_order == count_order[::-1]
        ranking = rankings.get_ranking(
            wf,
            order_by=[SortColumn(column_id='count', sort_desc=False)],
            include_all=True
        )
        rank_order = [e.run_id for e in ranking]
        assert rank_order == count_order
