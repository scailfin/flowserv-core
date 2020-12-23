# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Main module for the flowserv command line interface. Defines the top-level
command group and the API context for all commands.
"""

import click

from flowserv.client.cli.admin import configuration, init
from flowserv.client.cli.app import cli_app
from flowserv.client.cli.cleanup import cli_cleanup
from flowserv.client.cli.group import cli_group
from flowserv.client.cli.repository import list_repository
from flowserv.client.cli.run import cli_run
from flowserv.client.cli.uploads import cli_uploads
from flowserv.client.cli.user import cli_user, login_user, logout_user, whoami_user
from flowserv.client.cli.workflow import cli_workflow


@click.group()
def cli():
    """Command line interface for flowServ."""
    pass


# -- Administrative tasks (init, config, and cleanup) -------------------------
cli.add_command(configuration, name='config')
cli.add_command(init, name='init')

cli.add_command(cli_cleanup, name='cleanup')


# -- Applications -------------------------------------------------------------
cli.add_command(cli_app, name='app')


# -- Users --------------------------------------------------------------------
cli.add_command(login_user, name='login')
cli.add_command(logout_user, name='logout')
cli.add_command(cli_user, name='users')
cli.add_command(whoami_user, name='whoami')


# -- User groups --------------------------------------------------------------
cli.add_command(cli_group, 'groups')


# -- Group files --------------------------------------------------------------
cli.add_command(cli_uploads, 'files')


# -- Workflow repository listing ----------------------------------------------
cli.add_command(list_repository, name='repo')


# -- Workflows ----------------------------------------------------------------
cli.add_command(cli_workflow, name='workflows')


# -- Workflow Runs ------------------------------------------------------------
cli.add_command(cli_run, name='runs')
