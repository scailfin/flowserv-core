# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the app command-line interface."""

import os

from flowserv.client.cli.base import cli_flowserv as cli


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')


def test_app_installation(flowserv_cli):
    """Test installing, lisiting, and uninstalling a workflow app using the
    command-line interface.
    """
    # Create app in a fresh database
    cmd = ['app', 'install', '--key', 'mykey', TEMPLATE_DIR]
    result = flowserv_cli.invoke(cli, cmd)
    assert result.output.strip() == 'export FLOWSERV_APP=mykey'
    assert result.exit_code == 0
    app_key = result.output[result.output.rfind('=') + 1:].strip()
    # List apps
    cmd = ['workflows', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert app_key in result.output
    # Uninstall the app
    cmd = ['app', 'uninstall', app_key, '-f']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
