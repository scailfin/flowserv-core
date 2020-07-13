# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the command-line interface."""

import os

from flowserv.cli.admin import cli


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_create_workflow(cli_runner):
    """Test creating a new workflow via the command-line interface."""
    cmd = ['workflows', 'create', '-s', TEMPLATE_DIR]
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'export FLOWSERV_WORKFLOW=' in result.output
    # Create from GitHub repository
    cmd = ['workflows', 'create', '-r', 'Hello World']
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'export FLOWSERV_WORKFLOW=' in result.output
    # Error when using an invalid name.
    cmd = ['workflows', 'create', '-s', TEMPLATE_DIR, '-n', 'X'*600]
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == -1


def test_delete_workflow(cli_runner):
    """Test deleting a workflow via the command-line interface."""
    cmd = ['workflows', 'create', '-s', TEMPLATE_DIR]
    result = cli_runner.invoke(cli, cmd)
    pos = result.output.find('export FLOWSERV_WORKFLOW=') + 25
    workflow_id = result.output[pos:].strip()
    cmd = ['workflows', 'delete', workflow_id]
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'deleted workflow {}'.format(workflow_id) in result.output
    # -- Delete the same workflow again will raise error ----------------------
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == -1


def test_list_workflows(cli_runner):
    """Test listing workflows via the command-line interface."""
    # -- Test empty listing ---------------------------------------------------
    cmd = ['workflows', 'list']
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert result.output == ''
    # -- Test listing with two workflows --------------------------------------
    cmd = ['workflows', 'create', '-s', TEMPLATE_DIR]
    assert cli_runner.invoke(cli, cmd).exit_code == 0
    cmd = ['workflows', 'create', '-s', TEMPLATE_DIR]
    assert cli_runner.invoke(cli, cmd).exit_code == 0
    cmd = ['workflows', 'list']
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert result.output != ''


def test_update_workflow(cli_runner, tmpdir):
    """Test updating workflow properties via the command-line interface."""
    # -- Setup ----------------------------------------------------------------
    cmd = ['workflows', 'create', '-s', TEMPLATE_DIR]
    result = cli_runner.invoke(cli, cmd)
    pos = result.output.find('export FLOWSERV_WORKFLOW=') + 25
    workflow_id = result.output[pos:].strip()
    # -- Update without any arguments -----------------------------------------
    cmd = ['workflows', 'update', workflow_id]
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'nothing to update' in result.output
    # -- Update project instructions from file --------------------------------
    filename = os.path.join(tmpdir, 'instructions.txt')
    with open(filename, 'w') as f:
        f.write('My new instructions')
    cmd = ['workflows', 'update', '-i', filename, workflow_id]
    result = cli_runner.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'updated workflow {}'.format(workflow_id) in result.output
    # Ensure that new instructions appear in workflow listing.
    cmd = ['workflows', 'list']
    result = cli_runner.invoke(cli, cmd)
    assert 'My new instructions' in result.output
    # -- Error when updating an unknown workflow ------------------------------
    cmd = ['workflows', 'update', '-i', filename, 'UNKNOWN']
    assert cli_runner.invoke(cli, cmd).exit_code == -1
