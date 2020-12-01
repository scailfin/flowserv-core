# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the run command-line interface."""

from flowserv.client.cli.admin import cli


def test_delete_obsolete_runs(flowserv_cli):
    """Test deleting obsolete runs via the command-line interface."""
    cmd = ['runs', 'delete', '-d', '2020']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert '0 runs deleted.' in result.output


def test_list_obsolete_runs(flowserv_cli):
    """Test listing obsolete runs via the command-line interface."""
    # -- Test empty listing ---------------------------------------------------
    cmd = ['runs', 'list', '--before', '2020']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
