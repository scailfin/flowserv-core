# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the post-processing client module that is used to access files
that are included in the run result folder that is passed as input to a post-
processing workflow.
"""

import os

from flowserv.service.postproc.client import Runs
from flowserv.tests.service import create_ranking, create_user

import flowserv.service.postproc.util as postproc


def test_workflow_postproc_client(service, hello_world):
    """Test preparing and accessing post-processing results."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create four groups for the 'Hello World' workflow with one successful
    # run each.
    with service() as api:
        user_1 = create_user(api)
        workflow_id = hello_world(api)['id']
        create_ranking(api, workflow_id, user_1, 4)
    # -- Get ranking in decreasing order of avg_count. ------------------------
    with service() as api:
        ranking = api.ranking_manager.get_ranking(
            workflow=api.workflow_repository.get_workflow(workflow_id)
        )
        # Prepare data for the post-processing workflow.
        rundir = postproc.prepare_postproc_data(
            input_files=['results/analytics.json'],
            ranking=ranking,
            run_manager=api.run_manager
        )
        # Test the post-processing client that accesses the prepared data.
        runs = Runs(rundir)
        assert len(runs) == 4
        assert [r.run_id for r in ranking] == [r.run_id for r in runs]
        for i in range(len(runs)):
            run = runs.get_run(runs.at_rank(i).run_id)
            assert run.get_file(name='results/analytics.json') is not None
            assert os.path.isfile(run.get_file(name='results/analytics.json'))
            assert run.get_file(name='results/greeting.txt') is None
