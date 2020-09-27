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


def test_create_env_for_docker(tmpdir):
    """Create test environment with a Docker engine."""
    db = Flowserv(basedir=tmpdir, use_docker=True)
    assert isinstance(db.engine, DockerWorkflowEngine)


@pytest.mark.parametrize(
    'source,specfile,filekey',
    [
        (TEMPLATE_DIR, BENCHMARK_FILE, 'greetings'),
        ('helloworld', None, 'results/greetings.txt')
    ]
)
def test_run_helloworld_in_env(source, specfile, filekey, tmpdir):
    """Run the hello world workflow in the test environment."""
    db = Flowserv(basedir=os.path.join(tmpdir, 'flowserv'))
    # -- Install and run the workflow -----------------------------------------
    wf = db.install(source=source, specfile=specfile, ignore_postproc=True)
    run = wf.start_run({
        'names': StringIO('Alice\nBob\nClaire'),
        'greeting': 'Hey',
        'sleeptime': 0.1
    })
    assert run.is_success()
    text = run.get_file(filekey).open().read().decode('utf-8')
    assert 'Hey Alice' in text
    assert 'Hey Bob' in text
    assert 'Hey Claire' in text
    # -- Uninstall workflow ---------------------------------------------------
    db.uninstall(wf.identifier)
    # Running the workflow again will raise an error.
    with pytest.raises(err.UnknownWorkflowError):
        run = wf.start_run({'names': StringIO('Alice')})
    # Erase the workflow folser.
    db.erase()
    assert not os.path.exists(os.path.join(tmpdir, 'flowserv'))


def test_run_helloworld_with_postproc(tmpdir):
    """Run the hello world workflow in the test environment including the
    post-processing workflow.
    """
    db = Flowserv(basedir=os.path.join(tmpdir, 'flowserv'))
    # -- Install and run the workflow -----------------------------------------
    wf = db.install(source='helloworld', ignore_postproc=False)
    run = wf.start_run({
        'names': StringIO('Alice\nBob\nClaire'),
        'greeting': 'Hey',
        'sleeptime': 0.1
    })
    assert run.is_success()
    run = wf.start_run({
        'names': StringIO('Xenia\nYolanda\nZoe'),
        'greeting': 'Hello',
        'sleeptime': 0.1
    })
    assert run.is_success()
    postproc = wf.get_postproc_results()
    f = postproc.get_file('results/ngrams.csv')
    text = f.open().read().decode('utf-8')
    assert 'BOB,1' in text
    assert 'ELL,3' in text
    assert 'HEY,3' in text
    assert 'ZOE,1' in text
