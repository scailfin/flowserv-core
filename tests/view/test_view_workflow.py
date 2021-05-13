# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow resources view."""

import os

from flowserv.model.ranking import RunResult
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.view.workflow import WorkflowSerializer
from flowserv.view.validate import validator
from flowserv.volume.fs import FileSystemStorage

import flowserv.tests.model as model
import flowserv.util as util
import flowserv.view.workflow as labels


DIR = os.path.dirname(os.path.realpath(__file__))
# 'Hello World' benchmark directory.
BENCHMARK_DIR = os.path.join(DIR, '../.files/benchmark/postproc')
SPEC_FILE = os.path.join(BENCHMARK_DIR, 'benchmark.yaml')


def test_workflow_handle_serialization(database, tmpdir):
    """Test serialization of workflow handles."""
    schema = validator('WorkflowHandle')
    view = WorkflowSerializer()
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=FileSystemStorage(basedir=tmpdir))
        workflow = manager.create_workflow(
            source=BENCHMARK_DIR,
            name='Test',
            specfile=SPEC_FILE
        )
        doc = view.workflow_handle(workflow)
        schema.validate(doc)
        workflow = manager.get_workflow(workflow.workflow_id)
        schema.validate(doc)
        assert doc[labels.WORKFLOW_NAME] == 'Test'


def test_workflow_leaderboard_serialization(database, tmpdir):
    """Test serialization of a workflow leaderboard."""
    schema = validator('WorkflowLeaderboard')
    view = WorkflowSerializer()
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=FileSystemStorage(basedir=tmpdir))
        workflow = manager.create_workflow(
            source=BENCHMARK_DIR,
            name='Test',
            specfile=SPEC_FILE
        )
        ts = util.utc_now()
        ranking = [RunResult(
            run_id='0',
            group_id='1',
            group_name='A',
            created_at=ts,
            started_at=ts,
            finished_at=ts,
            values={'len': 1, 'count': 10}
        )]
        doc = view.workflow_leaderboard(workflow, ranking=ranking)
        schema.validate(doc)


def test_workflow_listing_serialization(database, tmpdir):
    """Test serialization of workflow listings."""
    schema = validator('WorkflowListing')
    view = WorkflowSerializer()
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=FileSystemStorage(basedir=tmpdir))
        model.create_workflow(session)
        model.create_workflow(session)
        workflows = manager.list_workflows()
        doc = view.workflow_listing(workflows)
        schema.validate(doc)
        assert len(doc[labels.WORKFLOW_LIST]) == 2
