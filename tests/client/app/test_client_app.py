# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the flowServ client environment."""

import os

from flowserv.client.app.base import Flowserv
from flowserv.config import Config
from flowserv.volume.manager import FStore


DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '..', '..', '.files', 'benchmark', 'helloworld')


def test_install_app(tmpdir):
    """Install and uninstall a workflow application via the app client."""
    basedir = os.path.join(tmpdir, 'test')
    env = Config()\
        .basedir(tmpdir)\
        .volume(FStore(basedir=str(tmpdir)))
    client = Flowserv(env=env)
    # Install workflow as application.
    app_id = client.install(source=BENCHMARK_DIR)
    # Get the application object.
    app = client.open(app_id)
    assert app.name() == 'Hello World'
    assert app.workflow_id == app.group_id
    # Use the open function to get a fresh instance of the workflow.
    app = Flowserv(env=app.service).open(identifier=app_id)
    assert app.name() == 'Hello World'
    assert app.workflow_id == app.group_id
    # Uninstall the app.
    client.uninstall(app_id)
    # Erase the nase directory.
    client.erase()
    assert not os.path.isdir(basedir)
