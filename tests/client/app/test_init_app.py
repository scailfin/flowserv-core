# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the flowServ test environment."""

from flowserv.client.app.base import Flowserv
from flowserv.controller.serial.docker import DockerWorkflowEngine


def test_create_env_for_docker(tmpdir):
    """Create test environment with a Docker engine."""
    db = Flowserv(basedir=tmpdir, docker=True)
    assert isinstance(db.service._engine, DockerWorkflowEngine)


def test_env_list_repository(tmpdir):
    """Test listing repository content from the flowserv environment."""
    db = Flowserv(basedir=tmpdir)
    assert db.repository() is not None
