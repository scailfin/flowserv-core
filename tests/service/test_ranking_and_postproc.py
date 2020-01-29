# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for workflow evaluation rankings."""

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
import flowserv.service.postproc.util as postproc


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default user
UID = '0000'


def init(basedir):
    """Initialize a database with two workflows and four groups each. Returns
    the service API, the workflow controller, and a dictionary containing for
    each workflow identifier a list or groups.
    """
    con = db.init_db(str(basedir), users=[UID]).connect()
    engine = StateEngine()
    api = API(con=con, engine=engine, basedir=str(basedir))
    # Create two workflows with two groups each
    workflows = dict()
    r = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    workflows[r['id']] = list()
    r = api.workflows().create_workflow(name='W2', sourcedir=TEMPLATE_DIR)
    workflows[r['id']] = list()
    # Create two groups for each workflow
    for w_id in workflows:
        for i in range(4):
            name = 'G{}'.format(i)
            r = api.groups().create_group(
                workflow_id=w_id,
                name=name,
                user_id=UID
            )
            workflows[w_id].append(r['id'])
    return api, engine, workflows


def test_workflow_result_ranking(tmpdir):
    """Test creating rankings from multiple workflow runs."""
    # Initialize the database and the API
    api, engine, workflows = init(tmpdir)
    # Insert two runs for each workflow group. For the first workflow we
    # increase the value for the ranking attribute avg_count with each run.
    # The final ranking for that workflow should have the groups in reverse
    # order of their appearance in the workflow's list. For the second workflow
    # we decrease the value of avg_count with each run so that the final
    # ranking contains groups in the same order as in the workflow's list.
    avg_count = 0.0
    inc = 10.0
    wf_list = list(workflows.keys())
    for w_id in wf_list:
        for g_id in workflows[w_id]:
            # Create a single fake upload file. We do not actually run the
            # workflow but need a valid file handle to avoid an error.
            r = api.uploads().upload_file(
                group_id=g_id,
                file=FakeStream(data=['Alice', 'Bob'], format='txt/plain'),
                name='names.txt',
                user_id=UID
            )
            file_id = r['id']
            r = api.runs().start_run(
                group_id=g_id,
                arguments=[{'id': 'names', 'value': file_id}],
                user_id=UID
            )
            r_id = r['id']
            engine.start(r_id)
            run = api.run_manager.get_run(r_id)
            data = {
                'avg_count': avg_count,
                'max_len': 10,
                'max_line': 'NO LINE'
            }
            avg_count += inc
            fso = FSObject(
                identifier=util.get_unique_identifier(),
                name='results/analytics.json',
                filename=FakeStream(data=data).save(
                    os.path.join(run.rundir, 'results/analytics.json')
                )
            )
            api.runs().update_run(
                run_id=r_id,
                state=engine.success(r_id, resources=[fso])
            )
        inc = -10.0
    reversed = True
    for w_id in wf_list:
        ranking = api.workflows().get_ranking(workflow_id=w_id)
        serialize.validate_ranking(ranking)
        groups = [r['group']['id'] for r in ranking['ranking']]
        if reversed:
            groups = groups[::-1]
        assert groups == workflows[w_id]
        reversed = not reversed


def test_workflow_postproc(tmpdir):
    """Test preparing and accessing post-processing results."""
    # Initialize the database and the API
    api, engine, workflows = init(tmpdir)
    # Insert two runs for each workflow group. Here, we will only consider the
    # results for the first workflow where we increase the value of the
    # avg_count value.
    avg_count = 0.0
    wf_list = list(workflows.keys())
    line = 'A'
    for w_id in wf_list:
        for g_id in workflows[w_id]:
            # Create a single fake upload file. We do not actually run the
            # workflow but need a valid file handle to avoid an error.
            r = api.uploads().upload_file(
                group_id=g_id,
                file=FakeStream(data=['Alice', 'Bob'], format='txt/plain'),
                name='names.txt',
                user_id=UID
            )
            file_id = r['id']
            r = api.runs().start_run(
                group_id=g_id,
                arguments=[{'id': 'names', 'value': file_id}],
                user_id=UID
            )
            r_id = r['id']
            engine.start(r_id)
            run = api.run_manager.get_run(r_id)
            data = {
                'avg_count': avg_count,
                'max_len': int(avg_count),
                'max_line': line
            }
            avg_count += 10.0
            line += 'A'
            fso = FSObject(
                identifier=util.get_unique_identifier(),
                name='results/analytics.json',
                filename=FakeStream(data=data).save(
                    os.path.join(run.rundir, 'results/analytics.json')
                )
            )
            api.runs().update_run(
                run_id=r_id,
                state=engine.success(r_id, resources=[fso])
            )
    w_id = wf_list[0]
    template = api.workflow_repository.get_workflow(workflow_id=w_id)
    ranking = api.ranking_manager.get_ranking(
        workflow_id=w_id,
        result_schema=template.get_schema()
    )
    rundir = postproc.prepare_postproc_data(
        input_files=['results/analytics.json'],
        ranking=ranking,
        run_manager=api.run_manager
    )
    for f in os.listdir(rundir):
        print(f)
