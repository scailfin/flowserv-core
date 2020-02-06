# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the post-processing client module that is used to access files
that are included in the run result folder that is passed as input to a post-
processing workflow.
"""

import os

from flowserv.model.workflow.resource import FSObject
from flowserv.service.postproc.client import Runs
from flowserv.tests.files import FakeStream

import flowserv.core.util as util
import flowserv.service.postproc.util as postproc
import flowserv.tests.service as service


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_workflow_postproc_client(tmpdir):
    """Test preparing and accessing post-processing results."""
    # Initialize the database and the API. Create two workflows and four groups
    # for each workflow.
    api, engine, workflows = service.init_service(
        basedir=str(tmpdir),
        templatedir=TEMPLATE_DIR,
        wf_count=2,
        gr_count=4
    )
    # Insert two runs for each workflow group. Here, we will only consider the
    # results for the first workflow where we increase the value of the
    # avg_count value.
    avg_count = 0.0
    line = 'A'
    for w_id, groups in workflows:
        for g_id, u_id in groups:
            # Create a single fake upload file. We do not actually run the
            # workflow but need a valid file handle to avoid an error.
            r = api.uploads().upload_file(
                group_id=g_id,
                file=FakeStream(data=['Alice', 'Bob'], format='txt/plain'),
                name='names.txt',
                user_id=u_id
            )
            file_id = r['id']
            r = api.runs().start_run(
                group_id=g_id,
                arguments=[{'id': 'names', 'value': file_id}],
                user_id=u_id
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
            f1 = FSObject(
                identifier=util.get_unique_identifier(),
                name='results/analytics.json',
                filename=FakeStream(data=data).save(
                    os.path.join(run.rundir, 'results/analytics.json')
                )
            )
            f2 = FSObject(
                identifier=util.get_unique_identifier(),
                name='results/greeting.txt',
                filename=FakeStream(data=['Hi Alice', 'Hi Bob']).save(
                    os.path.join(run.rundir, 'results/greeting.txt')
                )
            )
            api.runs().update_run(
                run_id=r_id,
                state=engine.success(r_id, resources=[f1, f2])
            )
    w_id, _ = workflows[0]
    # Get the workflow ranking. The ranking should contain four runs.
    template = api.workflow_repository.get_workflow(workflow_id=w_id)
    ranking = api.ranking_manager.get_ranking(
        workflow_id=w_id,
        result_schema=template.get_schema()
    )
    assert len(ranking) == 4
    # Prepare data for the post-processing workflow.
    rundir = postproc.prepare_postproc_data(
        input_files=['results/analytics.json'],
        ranking=ranking,
        run_manager=api.run_manager
    )
    # Test the post-processing client that accesses the prepared data.
    runs = Runs(rundir)
    assert len(runs) == 4
    assert [r.run_id for r in ranking] == [r.identifier for r in runs]
    for run in runs:
        assert run.get_file('results/analytics.json') is not None
        assert os.path.isfile(run.get_file('results/analytics.json'))
        assert run.get_file('results/greeting.txt') is None
