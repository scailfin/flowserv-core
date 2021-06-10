# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the flowserv application client."""

import os
import pytest

from io import BytesIO, StringIO

from flowserv.client.app.base import Flowserv
from flowserv.volume.fs import FSFile

import flowserv.error as err
import flowserv.model.workflow.state as st


DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '..', '..', '.files', 'benchmark', 'helloworld')
BENCHMARK_FILE = os.path.join(BENCHMARK_DIR, 'benchmark-with-outputs.yaml')
POSTPROC_SPEC = os.path.join(BENCHMARK_DIR, '..', 'postproc', 'benchmark.yaml')


def test_run_helloworld_in_env(tmpdir):
    """Run the hello world workflow in the test environment."""
    basedir = os.path.join(tmpdir, 'flowserv')
    db = Flowserv(basedir=basedir, open_access=True, clear=True)
    # -- Install and run the workflow -----------------------------------------
    app_id = db.install(
        source=BENCHMARK_DIR,
        specfile=BENCHMARK_FILE,
        ignore_postproc=True
    )
    wf = db.open(app_id)
    run = wf.start_run({
        'names': StringIO('Alice\nBob\nClaire'),
        'greeting': 'Hey',
        'sleeptime': 0.1
    })
    assert run.is_success()
    assert not run.is_active()
    assert str(run) == st.STATE_SUCCESS
    assert len(run.files()) == 2
    text = run.get_file('greetings').text()
    assert 'Hey Alice' in text
    assert 'Hey Bob' in text
    assert 'Hey Claire' in text
    # There should no by any post-processing results
    assert wf.get_postproc_results() is None
    # -- Polling the run should return a valid result -------------------------
    run = wf.poll_run(run.run_id)
    assert run.is_success()
    assert not run.is_active()
    assert str(run) == st.STATE_SUCCESS
    assert len(run.files()) == 2
    file_handles = dict()
    for f in run.files():
        file_handles[f.name] = f
    assert file_handles['greetings'].name == 'greetings'
    assert file_handles['greetings'].format == {'type': 'plaintext'}
    assert file_handles['results/analytics.json'].name == 'results/analytics.json'
    assert file_handles['results/analytics.json'].caption is None
    assert file_handles['results/analytics.json'].format == {'type': 'json'}
    # -- Cancelling a finished run raises an error ----------------------------
    with pytest.raises(err.InvalidRunStateError):
        wf.cancel_run(run.run_id)
    # -- Uninstall workflow ---------------------------------------------------
    db.uninstall(wf.identifier)
    # Running the workflow again will raise an error.
    with pytest.raises(err.UnknownWorkflowGroupError):
        run = wf.start_run({'names': StringIO('Alice')})
    # Erase the workflow folser.
    db.erase()
    assert not os.path.exists(os.path.join(tmpdir, 'flowserv'))


def test_run_helloworld_with_diff_inputs(tmpdir):
    """Run the hello world workflow in the test environment with different
    types of input files.
    """
    basedir = os.path.join(tmpdir, 'flowserv')
    db = Flowserv(basedir=basedir, open_access=True, clear=True)
    files = list()
    files.append(BytesIO(b'Alice\nBob\nClaire'))
    filename = os.path.join(tmpdir, 'names.txt')
    with open(filename, 'w') as f:
        f.write('Alice\nBob\nClaire')
    files.append(filename)
    files.append(FSFile(filename))
    # -- Install and run the workflow -----------------------------------------
    app_id = db.install(source=BENCHMARK_DIR, ignore_postproc=True)
    wf = db.open(app_id)
    for file in files:
        run = wf.start_run({
            'names': file,
            'greeting': 'Hey',
            'sleeptime': 0.0
        })
        assert run.is_success()
    # -- Error for invalid file type ------------------------------------------
    with pytest.raises(err.InvalidArgumentError):
        wf.start_run({
            'names': ['A', 'B'],
            'greeting': 'Hey',
            'sleeptime': 0.1
        })
    # -- Error for unknown parameter ------------------------------------------
    with pytest.raises(err.UnknownParameterError):
        wf.start_run({
            'names': filename,
            'greeting': 'Hey',
            'sleep': 0.1
        })


def test_run_helloworld_with_postproc(tmpdir):
    """Run the hello world workflow in the test environment including the
    post-processing workflow.
    """
    db = Flowserv(basedir=os.path.join(tmpdir, 'flowserv'), open_access=True)
    # -- Install and run the workflow -----------------------------------------
    app_id = db.install(source=BENCHMARK_DIR, specfile=POSTPROC_SPEC, ignore_postproc=False)
    wf = db.open(app_id)
    run = wf.start_run({
        'names': StringIO('Alice\nBob\nClaire'),
        'greeting': 'Hey'
    })
    assert run.is_success()
    run = wf.start_run({
        'names': StringIO('Xenia\nYolanda\nZoe'),
        'greeting': 'Hello'
    })
    assert run.is_success()
    postproc = wf.get_postproc_results()
    doc = postproc.get_file('results/compare.json').json()
    assert doc == [{'avg_count': 12.0, 'total_count': 36, 'max_len': 14, 'max_line': 'Hello Yolanda!'}]
    # Access the data object again to receive the buffered data
    doc = postproc.get_file('results/compare.json').json()
    assert doc == [{'avg_count': 12.0, 'total_count': 36, 'max_len': 14, 'max_line': 'Hello Yolanda!'}]
