# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for flowserv client configuration."""

import os

from flowserv.config.client import (
    ACCESS_TOKEN, BENCHMARK_ID, FLOWSERV_ACCESS_TOKEN, FLOWSERV_CLIENT, CLIENT,
    ROB_BENCHMARK, ROB_SUBMISSION, SUBMISSION_ID
)


def test_access_token():
    """Test getting the access token from the environment."""
    os.environ[FLOWSERV_ACCESS_TOKEN] = '0000'
    assert ACCESS_TOKEN() == '0000'
    del os.environ[FLOWSERV_ACCESS_TOKEN]


def test_benchmark_identifier():
    """Test getting the benchmark identifier from the environment."""
    os.environ[ROB_BENCHMARK] = 'BM0000'
    assert BENCHMARK_ID() == 'BM0000'
    del os.environ[ROB_BENCHMARK]
    assert BENCHMARK_ID() is None
    assert BENCHMARK_ID(default='DBM000') == 'DBM000'


def test_client():
    """Test getting the client type identifier from the environment."""
    os.environ[FLOWSERV_CLIENT] = 'REMOTE'
    assert CLIENT() == 'REMOTE'
    del os.environ[FLOWSERV_CLIENT]
    assert CLIENT() == 'LOCAL'


def test_submission_identifier():
    """Test getting the submission identifier from the environment."""
    os.environ[ROB_SUBMISSION] = 'SM0000'
    assert SUBMISSION_ID() == 'SM0000'
    del os.environ[ROB_SUBMISSION]
    assert SUBMISSION_ID() is None
    assert SUBMISSION_ID(default='DSM000') == 'DSM000'
