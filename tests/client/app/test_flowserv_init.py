# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for initializing the flowServ client environment."""

import os

from flowserv.client.app.base import Flowserv
from flowserv.model.database import TEST_URL


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')


def test_install_app(tmpdir):
    """Test initializing the Flowserv client with the full set of arguments."""
    basedir = os.path.join(tmpdir, 'test')
    Flowserv(
        basedir=basedir,
        database=TEST_URL,
        open_access=True,
        run_async=True,
        clear=True
    )
