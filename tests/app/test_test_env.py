# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the flowServ test environment."""

import os
import pytest

from io import StringIO

from flowserv.controller.serial.docker import DockerWorkflowEngine
from flowserv.tests.workflow import Flowserv

import flowserv.error as err


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')
BENCHMARK_FILE = os.path.join(TEMPLATE_DIR, 'benchmark-with-outputs.yaml')


def test_run_helloworld_in_test_env(tmpdir):
    """Run the hello world workflow in the test environment."""
    db = Flowserv(basedir=os.path.join(tmpdir, 'flowserv'))
    # -- Install and run the workflow -----------------------------------------
    wf = db.install(source=TEMPLATE_DIR, specfile=BENCHMARK_FILE)
    run = wf.start_run({
        'names': StringIO('Alice\nBob\nClaire'),
        'greeting': 'Hey',
        'sleeptime': 0.1
    })
    assert run.is_success()
    text = run.get_file('greetings').open().read().decode('utf-8')
    assert text.strip() == 'Hey Alice!\nHey Bob!\nHey Claire!'
    # -- Uninstall workflow ---------------------------------------------------
    db.uninstall(wf.identifier)
    # Running the workflow again will raise an error.
    with pytest.raises(err.UnknownWorkflowError):
        run = wf.start_run({'names': StringIO('Alice')})
    # Erase the workflow folser.
    db.erase()
    assert not os.path.exists(os.path.join(tmpdir, 'flowserv'))


def test_create_env_for_docker(tmpdir):
    """Create test environment with a Docker engine."""
    db = Flowserv(basedir=tmpdir, use_docker=True)
    assert isinstance(db.engine, DockerWorkflowEngine)
