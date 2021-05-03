# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow command-line interface."""

import os

from flowserv.client.cli.base import cli_flowserv as cli
from flowserv.client.cli.workflow import read_instructions


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '..', '..', '.files', 'benchmark', 'helloworld')
BENCHMARK_FILE = os.path.join(DIR, '..', '..', '.files', 'benchmark', 'postproc', 'benchmark-no-params.yaml')


def test_list_workflows(flowserv_cli):
    """Test listing workflows via the command-line interface."""
    # -- Test empty listing ---------------------------------------------------
    cmd = ['workflows', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert result.output == ''
    # -- Test listing with two workflows --------------------------------------
    cmd = ['app', 'install', TEMPLATE_DIR]
    assert flowserv_cli.invoke(cli, cmd).exit_code == 0
    cmd = ['app', 'install', TEMPLATE_DIR]
    assert flowserv_cli.invoke(cli, cmd).exit_code == 0
    cmd = ['workflows', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert result.output != ''


def test_update_workflow(flowserv_cli, tmpdir):
    """Test updating workflow properties via the command-line interface."""
    # -- Setup ----------------------------------------------------------------
    cmd = ['app', 'install', TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    pos = result.output.find('export FLOWSERV_APP=') + 20
    workflow_id = result.output[pos:].strip()
    # -- Update without any arguments -----------------------------------------
    cmd = ['workflows', 'update', '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'nothing to update' in result.output
    # -- Update project instructions from file --------------------------------
    filename = os.path.join(tmpdir, 'instructions.txt')
    with open(filename, 'w') as f:
        f.write('My new instructions')
    cmd = ['workflows', 'update', '-i', filename, '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'updated workflow {}'.format(workflow_id) in result.output
    # Ensure that new instructions appear in workflow listing.
    cmd = ['workflows', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert 'My new instructions' in result.output
    # -- Error when updating an unknown workflow ------------------------------
    cmd = ['workflows', 'update', '-i', filename, '-w', 'UNKNOWN']
    result = flowserv_cli.invoke(cli, cmd)
    assert str(result.exception) == "unknown workflow 'UNKNOWN'"
    assert result.exit_code == 1


def test_read_instructions(tmpdir):
    """Test read instructions function."""
    assert read_instructions(None) is None
    filename = os.path.join(tmpdir, 'instructions.txt')
    with open(filename, 'wt') as f:
        f.write('abc')
    assert read_instructions(filename) == 'abc'


def test_workflow_lifecycle(flowserv_cli, tmpdir):
    """Test workflow life cycle via the command-line interface."""
    # -- Create workflow ------------------------------------------------------
    cmd = ['workflows', 'create', TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    workflow_id = result.output[result.output.rfind('=') + 1:].strip()
    # -- Get workflow ---------------------------------------------------------
    cmd = ['workflows', 'show', '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- Delete workflow ------------------------------------------------------
    cmd = ['workflows', 'delete', '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0


def test_workflow_result_files(flowserv_cli, tmpdir):
    """Test running the hello world workflow and downloading the result files."""
    # -- Setup ----------------------------------------------------------------
    cmd = ['app', 'install', '-s', BENCHMARK_FILE, TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    pos = result.output.find('export FLOWSERV_APP=') + 20
    workflow_id = result.output[pos:].strip()
    # -- Result files not available pre-run -----------------------------------
    filename = os.path.join(tmpdir, 'dummy.file')
    cmd = ['workflows', 'download', 'file', '-f', '0000', '-o', filename, '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 1
    cmd = ['workflows', 'download', 'archive', '-o', filename, '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 1
    # -- Run workflow ---------------------------------------------------------
    cmd = ['runs', 'start', '-g', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- Get workflow ---------------------------------------------------------
    cmd = ['workflows', 'show', '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'Post-processing' in result.output
    for line in result.output.split('\n'):
        if '(results/compare.json)' in line:
            file_id = line.split()[0]
    # -- Rankings -------------------------------------------------------------
    cmd = ['workflows', 'ranking', '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    cmd = ['workflows', 'ranking', '-a', '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- Result files ---------------------------------------------------------
    filename = os.path.join(tmpdir, 'compare.json')
    assert not os.path.isfile(filename)
    cmd = ['workflows', 'download', 'file', '-f', file_id, '-o', filename, '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert os.path.isfile(filename)
    filename = os.path.join(tmpdir, 'archive.tar.gz')
    assert not os.path.isfile(filename)
    cmd = ['workflows', 'download', 'archive', '-o', filename, '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert os.path.isfile(filename)
    # -- Update without any arguments -----------------------------------------
    cmd = ['workflows', 'update', '-d', 'New description', '-w', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
