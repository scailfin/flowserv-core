# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the manager that maintains workflow result rankings."""

import os
import pytest

from flowserv.model.group import WorkflowGroupManager
from flowserv.model.ranking.manager import RankingManager
from flowserv.model.run.manager import RunManager
from flowserv.model.template.schema import ResultColumn, ResultSchema, SortColumn
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.resource import FSObject, ResourceSet

import flowserv.error as err
import flowserv.util as util
import flowserv.model.parameter.declaration as pd
import flowserv.tests.db as db


"""Unique identifier for users and workflow templates."""
USER_1 = '0000'
WORKFLOW_1 = '0000'
WORKFLOW_2 = '0001'


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


def init(basedir):
    """Create a fresh database with one user, two workflows, and three groups
    for each workflow. Returns a ranking manager, a run manager, as well as the
    handles for the created groups.
    """
    # Create new database with three users
    connector = db.init_db(
        str(basedir),
        workflows=[WORKFLOW_1, WORKFLOW_2],
        users=[USER_1]
    )
    con = connector.connect()
    workflowdir = os.path.join(str(basedir), 'workflows')
    group_manager = WorkflowGroupManager(
        con=con,
        fs=WorkflowFileSystem(workflowdir)
    )
    handles = list()
    for workflow_id in [WORKFLOW_1, WORKFLOW_2]:
        for i in range(3):
            g = group_manager.create_group(
                workflow_id=workflow_id,
                name='Group {}'.format(i),
                user_id=USER_1,
                parameters=dict(),
                workflow_spec=dict()
            )
            handles.append(g)
    run_manager = RunManager(
        con=con,
        fs=WorkflowFileSystem(workflowdir)
    )
    return RankingManager(con), run_manager, handles[0:3], handles[3:]


def insert_run(
    workflow_id, group_id, result_schema, run_manager, ranking_manager, data
):
    """Insert a successful run with the given result data into the database.
    Return the ranking after the run was insterted.
    """
    r = run_manager.create_run(
        workflow_id=workflow_id,
        group_id=group_id,
        arguments=dict()
    )
    rundir = run_manager.fs.run_basedir(workflow_id, group_id, r.identifier)
    filename = os.path.join(rundir, RESULT_FILE_ID)
    util.write_object(filename=filename, obj=data)
    ranking_manager.insert_result(
        workflow_id=workflow_id,
        result_schema=result_schema,
        run_id=r.identifier,
        resources=ResourceSet([
            FSObject(
                identifier=util.get_unique_identifier(),
                name=RESULT_FILE_ID,
                filename=filename
            )
        ])
    )
    return ranking_manager.get_ranking(
        workflow_id=workflow_id,
        result_schema=result_schema
    )


def test_result_rankings(tmpdir):
    """Test ranking results for multiple workflow runs."""
    manager, run_manager, handles_1, handles_2 = init(tmpdir)
    # Register the two workflow schemas
    manager.register_workflow(workflow_id=WORKFLOW_1, result_schema=SCHEMA_1)
    manager.register_workflow(workflow_id=WORKFLOW_2, result_schema=SCHEMA_2)
    # Upload two files for each group in workflow 1 and insert the results into
    # the ranking database
    results = list([
        [handles_1[0].identifier],
        [handles_1[1].identifier, handles_1[0].identifier],
        [handles_1[2].identifier, handles_1[1].identifier, handles_1[0].identifier],
        [handles_1[0].identifier, handles_1[2].identifier, handles_1[1].identifier],
        [handles_1[1].identifier, handles_1[0].identifier, handles_1[2].identifier],
        [handles_1[2].identifier, handles_1[1].identifier, handles_1[0].identifier]
    ])
    results = results[::-1]
    for c in [2, 15]:
        for gh in handles_1:
            ranking = insert_run(
                workflow_id=WORKFLOW_1,
                result_schema=SCHEMA_1,
                group_id=gh.identifier,
                run_manager=run_manager,
                ranking_manager=manager,
                data={'count': c, 'avg': 1.0/float(c), 'name': 'N{}'.format(c)}
            )
            c += 1
            assert [r.group_id for r in ranking] == results.pop()
    # Include all results
    ranking = manager.get_ranking(
        workflow_id=WORKFLOW_1,
        result_schema=SCHEMA_1,
        include_all=True
    )
    assert len(ranking) == 6
    group_order = [r.group_id for r in ranking]
    assert group_order == [
        handles_1[2].identifier,
        handles_1[1].identifier,
        handles_1[0].identifier,
        handles_1[2].identifier,
        handles_1[1].identifier,
        handles_1[0].identifier
    ]
    # Upload two files for each group in workflow 2 and insert the results into
    # the ranking database
    results = list([
        [handles_2[0].identifier],
        [handles_2[0].identifier, handles_2[1].identifier],
        [handles_2[0].identifier, handles_2[1].identifier, handles_2[2].identifier],
        [handles_2[0].identifier, handles_2[1].identifier, handles_2[2].identifier],
        [handles_2[0].identifier, handles_2[1].identifier, handles_2[2].identifier],
        [handles_2[0].identifier, handles_2[1].identifier, handles_2[2].identifier]
    ])
    results = results[::-1]
    for c in [100, 200]:
        x = 0
        for gh in handles_2:
            ranking = insert_run(
                workflow_id=WORKFLOW_2,
                result_schema=SCHEMA_2,
                group_id=gh.identifier,
                run_manager=run_manager,
                ranking_manager=manager,
                data={'min': c-x, 'max': 1, 'values': {'min': c+x, 'max': c+x}}
            )
            x += 10
            assert [r.group_id for r in ranking] == results.pop()
        # Error when using the wrong schema
        with pytest.raises(err.ConstraintViolationError):
            ranking = insert_run(
                workflow_id=WORKFLOW_1,
                result_schema=SCHEMA_1,
                group_id=gh.identifier,
                run_manager=run_manager,
                ranking_manager=manager,
                data={'min': c-x, 'max': c-x, 'values': {'min': c+x, 'max': c+x}}
            )
    # Different sort oder
    ranking = manager.get_ranking(
        workflow_id=WORKFLOW_2,
        result_schema=SCHEMA_2,
        order_by=[
            SortColumn(identifier='max', sort_desc=True),
            SortColumn(identifier='min', sort_desc=True)
        ]
    )
    assert len(ranking) == 3
    group_order = [r.group_id for r in ranking]
    assert group_order == [
        handles_2[2].identifier,
        handles_2[1].identifier,
        handles_2[0].identifier
    ]
    # Error for unknown sore column
    with pytest.raises(err.InvalidSortColumnError):
        manager.get_ranking(
            workflow_id=WORKFLOW_2,
            result_schema=SCHEMA_2,
            order_by=[
                SortColumn(identifier='max', sort_desc=True),
                SortColumn(identifier='avg', sort_desc=True)
            ]
        )
