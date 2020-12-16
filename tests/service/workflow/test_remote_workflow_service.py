# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the remote workflow service API."""

from flowserv.model.template.schema import SortColumn


def test_get_ranking_remote(remote_service, mock_response):
    """Test getting leaderboard from remote service."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.workflows().get_ranking(workflow_id='0000')
    remote_service.workflows().get_ranking(
        workflow_id='0000',
        order_by=[SortColumn('A'), SortColumn('B', sort_desc=False)]
    )


def test_get_workflow_remote(remote_service, mock_response):
    """Test getting a workflow handle from the remote service."""
    remote_service.workflows().get_workflow(workflow_id='0000')


def test_list_workflows_remote(remote_service, mock_response):
    """Test getting a workflow listing from the remote service."""
    remote_service.workflows().list_workflows()
