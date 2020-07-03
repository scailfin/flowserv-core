# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for workflow evaluation rankings."""

import os

from flowserv.model.workflow.resource import FSObject
from flowserv.tests.files import FakeStream

import flowserv.util as util
import flowserv.tests.serialize as serialize
import flowserv.tests.service as service


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_workflow_result_ranking(tmpdir):
    """Test creating rankings from multiple workflow runs."""
    # Initialize the database and the API
    api, engine, workflows = service.init_service(
        basedir=str(tmpdir),
        templatedir=TEMPLATE_DIR,
        wf_count=2,
        gr_count=4
    )
    # Insert two runs for each workflow group. For the first workflow we
    # increase the value for the ranking attribute avg_count with each run.
    # The final ranking for that workflow should have the groups in reverse
    # order of their appearance in the workflow's list. For the second workflow
    # we decrease the value of avg_count with each run so that the final
    # ranking contains groups in the same order as in the workflow's list.
    avg_count = 0.0
    inc = 10.0
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
    for w_id, groups in workflows:
        ranking = api.workflows().get_ranking(workflow_id=w_id)
        serialize.validate_ranking(ranking)
        group_ids = [r['group']['id'] for r in ranking['ranking']]
        if reversed:
            group_ids = group_ids[::-1]
        assert group_ids == [g_id for g_id, _ in groups]
        reversed = not reversed
