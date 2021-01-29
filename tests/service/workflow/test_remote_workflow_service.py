# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the remote workflow service API."""

from flowserv.model.template.schema import SortColumn


def test_download_result_archive_remote(remote_service, mock_response):
    """Test downloading post-processing run result archive from the remote
    service.
    """
    remote_service.workflows().get_result_archive(workflow_id='0000')


def test_download_result_file_remote(remote_service, mock_response):
    """Test downloading a post-processing run result file from the remote
    service.
    """
    remote_service.workflows().get_result_file(workflow_id='0000', file_id='0001')


def test_get_ranking_remote(remote_service, mock_response):
    """Test getting leaderboard from remote service."""
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
