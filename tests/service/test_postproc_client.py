# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the post-processing client module that is used to access files
that are included in the run result folder that is passed as input to a post-
processing workflow.
"""

import os

from flowserv.service.postproc.base import RUNS_FILE, prepare_postproc_data
from flowserv.service.postproc.client import Runs
from flowserv.tests.service import create_ranking, create_user
from flowserv.volume.fs import FileSystemStorage

import flowserv.util as util


def test_empty_run_set(tmpdir):
    """Test initializing a client for an empty run set."""
    filename = os.path.join(tmpdir, RUNS_FILE)
    util.write_object(filename=filename, obj=[])
    runs = Runs(tmpdir)
    assert runs.get_run('0000') is None


def test_workflow_postproc_client(local_service, hello_world, tmpdir):
    """Test preparing and accessing post-processing results."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create four groups for the 'Hello World' workflow with one successful
    # run each.
    with local_service() as api:
        user_1 = create_user(api)
        workflow_id = hello_world(api).workflow_id
    with local_service(user_id=user_1) as api:
        create_ranking(api, workflow_id, 4)
    # -- Get ranking in decreasing order of avg_count. ------------------------
    with local_service(user_id=user_1) as api:
        workflow = api.workflows().workflow_repo.get_workflow(workflow_id)
        ranking = api.workflows().ranking_manager.get_ranking(workflow)
        # Prepare data for the post-processing workflow.
        prepare_postproc_data(
            input_files=['results/analytics.json'],
            ranking=ranking,
            run_manager=api.runs().run_manager,
            store=FileSystemStorage(basedir=os.path.join(tmpdir, 'postproc_run'))
        )
        # Test the post-processing client that accesses the prepared data.
        runs = Runs(os.path.join(tmpdir, 'postproc_run'))
        assert len(runs) == 4
        assert [r.run_id for r in ranking] == [r.run_id for r in runs]
        for i in range(len(runs)):
            run = runs.get_run(runs.at_rank(i).run_id)
            assert run.get_file(name='results/analytics.json') is not None
            assert os.path.isfile(run.get_file(name='results/analytics.json'))
            assert run.get_file(name='results/greeting.txt') is None
