# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow command-line interface."""

import os

from flowserv.cli.admin import cli


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')
NOPARAM_BENCHMARK = os.path.join(DIR, '../.files/benchmark/postproc/noparam.yaml')  # noqa: E501


def test_list_workflows(flowserv_cli):
    """Test listing workflows via the command-line interface."""
    # -- Test empty listing ---------------------------------------------------
    cmd = ['workflows', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert result.output == ''
    # -- Test listing with two workflows --------------------------------------
    cmd = ['install', TEMPLATE_DIR]
    assert flowserv_cli.invoke(cli, cmd).exit_code == 0
    cmd = ['install', TEMPLATE_DIR]
    assert flowserv_cli.invoke(cli, cmd).exit_code == 0
    cmd = ['workflows', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert result.output != ''


def test_run_workflow(flowserv_cli, tmpdir):
    """Test running a workflow from the command line. We use a template with
    no input parameters to not block the execution for reading parameters.
    """
    # -- Setup ----------------------------------------------------------------
    cmd = ['install', TEMPLATE_DIR, '-s', NOPARAM_BENCHMARK]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    pos = result.output.find('export FLOWSERV_APP=') + 20
    workflow_id = result.output[pos:].strip()
    # -- Run the workflow -----------------------------------------------------
    cmd = ['run', workflow_id, '-v']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'results/greetings.txt' in result.output
    assert 'results/analytics.json' in result.output
    assert 'results/compare.json' in result.output
    # Save output files
    outdir = os.path.join(tmpdir, '.out')
    cmd = ['run', workflow_id, '-o', outdir]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert os.path.isfile(os.path.join(outdir, 'results/greetings.txt'))
    assert os.path.isfile(os.path.join(outdir, 'results/analytics.json'))
    assert os.path.isfile(os.path.join(outdir, 'results/compare.json'))


def test_update_workflow(flowserv_cli, tmpdir):
    """Test updating workflow properties via the command-line interface."""
    # -- Setup ----------------------------------------------------------------
    cmd = ['install', TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    pos = result.output.find('export FLOWSERV_APP=') + 20
    workflow_id = result.output[pos:].strip()
    # -- Update without any arguments -----------------------------------------
    cmd = ['workflows', 'update', workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'nothing to update' in result.output
    # -- Update project instructions from file --------------------------------
    filename = os.path.join(tmpdir, 'instructions.txt')
    with open(filename, 'w') as f:
        f.write('My new instructions')
    cmd = ['workflows', 'update', '-i', filename, workflow_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'updated workflow {}'.format(workflow_id) in result.output
    # Ensure that new instructions appear in workflow listing.
    cmd = ['workflows', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert 'My new instructions' in result.output
    # -- Error when updating an unknown workflow ------------------------------
    cmd = ['workflows', 'update', '-i', filename, 'UNKNOWN']
    assert flowserv_cli.invoke(cli, cmd).exit_code == -1
