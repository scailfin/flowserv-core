# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the run command-line interface."""

import os

from flowserv.client.cli.base import cli


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')
BENCHMARK_FILE = os.path.join(DIR, '../../.files/benchmark/postproc/benchmark-no-params.yaml')


def test_run_lifecycle(flowserv_cli):
    """Test creating, running, listing, canceling, and deleting workflow runs."""
    # -- Setup ----------------------------------------------------------------
    cmd = ['app', 'install', '-g', '-s', BENCHMARK_FILE, TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    pos = result.output.find('export FLOWSERV_APP=') + 20
    workflow_id = result.output[pos:].strip()
    # -- Run workflow ---------------------------------------------------------
    cmd = ['runs', 'start', '-g', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    run_id = result.output.strip().split()[2]
    # -- Cancel run (raises error since successful runs cannot be canceled) ---
    cmd = ['runs', 'cancel', run_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exception is not None
    assert result.exit_code == 1
    # -- List runs ------------------------------------------------------------
    cmd = ['runs', 'list', '-g', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert run_id in result.output
    # -- Delete run ------------------------------------------------------------
    cmd = ['runs', 'delete', run_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert run_id in result.output
    # -- List runs ------------------------------------------------------------
    cmd = ['runs', 'list', '-g', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert run_id not in result.output


def test_run_result_files(flowserv_cli, tmpdir):
    """Test running the hello world workflow and downloading the result files."""
    # -- Setup ----------------------------------------------------------------
    cmd = ['app', 'install', '-g', '-s', BENCHMARK_FILE, TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    pos = result.output.find('export FLOWSERV_APP=') + 20
    workflow_id = result.output[pos:].strip()
    # -- Run workflow ---------------------------------------------------------
    cmd = ['runs', 'start', '-g', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    run_id = result.output.strip().split()[2]
    # -- Show run -------------------------------------------------------------
    cmd = ['runs', 'show', run_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    for line in result.output.split('\n'):
        if '(results/greetings.txt)' in line:
            file_id = line.split()[0]
    # -- Get greeting result file ---------------------------------------------
    filename = os.path.join(tmpdir, 'greetings.txt')
    assert not os.path.isfile(filename)
    cmd = ['runs', 'download', 'file', '-f', file_id, '-o', filename, run_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert os.path.isfile(filename)
    filename = os.path.join(tmpdir, 'run_archive.tar.gz')
    assert not os.path.isfile(filename)
    cmd = ['runs', 'download', 'archive', '-o', filename, run_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert os.path.isfile(filename)
