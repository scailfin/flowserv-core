# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the group command-line interface."""

import os

from flowserv.client.cli.base import cli_flowserv as cli

import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')


def test_cli_group_files(flowserv_cli, tmpdir):
    """Test CLI workflow group files command."""
    # Create app in a fresh database.
    cmd = ['app', 'install', '--key', 'mykey', TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    app_key = result.output[result.output.rfind('=') + 1:].strip()
    # -- Upload file ----------------------------------------------------------
    filename = os.path.join(tmpdir, 'myfile.json')
    util.write_object(filename=filename, obj=[])
    cmd = ['files', 'upload', '-g', app_key, '-i', filename]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'myfile.json' in result.output
    file_id = result.output.split()[-1][:-1]
    # Ensure that the file is included in the group descriptor
    cmd = ['groups', 'show', '-g', app_key]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert file_id in result.output
    # -- List files -----------------------------------------------------------
    cmd = ['files', 'list', '-g', app_key]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'myfile.json' in result.output
    # -- Download file --------------------------------------------------------
    cmd = ['files', 'download', '-g', app_key, '-f', file_id, '-o', filename]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- Delete file ----------------------------------------------------------
    cmd = ['files', 'delete', '-g', app_key, '-f', file_id, '--force']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0


def test_cli_group_lifecycle(flowserv_cli):
    """Test CLI workflow group commands."""
    # Create app in a fresh database
    cmd = ['app', 'install', '--key', 'mykey', TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    app_key = result.output[result.output.rfind('=') + 1:].strip()
    # -- Create Group ---------------------------------------------------------
    cmd = ['groups', 'create', '-w', app_key, '-n', 'G1']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    group_id = result.output[result.output.rfind('=') + 1:].strip()
    # Error when workflow identifier is missing.
    cmd = ['groups', 'create', '-n', 'G1']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 1  # MissingConfigurationError
    # -- Show Group -----------------------------------------------------------
    cmd = ['groups', 'show', '-g', group_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert 'Name    : G1' in result.output
    assert result.exit_code == 0
    # Error for missing configuration.
    cmd = ['groups', 'show']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 1  # MissingConfigurationError
    # -- Update Group ---------------------------------------------------------
    cmd = ['groups', 'update', '-g', group_id, '-n', 'G2']
    result = flowserv_cli.invoke(cli, cmd)
    assert 'Name    : G2' in result.output
    assert result.exit_code == 0
    # Error cases for group updates.
    cmd = ['groups', 'update', '-n', 'G3']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 1  # MissingConfigurationError
    cmd = ['groups', 'update', '-g', group_id]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 2  # UsageError
    # -- Delete Group ---------------------------------------------------------
    cmd = ['groups', 'delete', '-g', group_id, '-f']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # Error for missing group identifier.
    cmd = ['groups', 'delete', '-f']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 1  # MissingConfigurationError


def test_cli_group_list(flowserv_cli):
    """Test CLI workflow group list command."""
    # Create app in a fresh database.
    cmd = ['app', 'install', '--key', 'mykey', TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- List all groups ------------------------------------------------------
    cmd = ['groups', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
