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
