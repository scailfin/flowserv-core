# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the administrative tasks to configure the
environment, intialize the underlying database, and to create and maintain
workflows in the repository.
"""

import click
import os
import sys

from flowserv.cli.app import install_application, uninstall_application
from flowserv.cli.config import get_configuration
from flowserv.cli.repository import list_repository
from flowserv.cli.run import runscli
from flowserv.cli.user import register_user
from flowserv.cli.workflow import run_workflow, workflowcli
from flowserv.config.api import API_BASEDIR
from flowserv.model.database import DB

import flowserv.error as err


@click.group()
def cli():
    """Command line interface for administrative tasks to manage a flowServ
    instance.
    """
    pass


@cli.command(name='config')
def configuration():
    """Print configuration variables for flowServ."""
    comment = '\n#\n# {}\n#\n'
    envvar = 'export {}={}'
    for title, envs in get_configuration().items():
        click.echo(comment.format(title))
        for var, val in envs.items():
            click.echo(envvar.format(var, val))
    click.echo()


@cli.command()
@click.option(
    '-f', '--force',
    is_flag=True,
    default=False,
    help='Create database without confirmation'
)
def init(force=False):
    """Initialize database and base directories for the API."""
    if not force:
        click.echo('This will erase an existing database.')
        click.confirm('Continue?', default=True, abort=True)
    # Create a new instance of the database
    try:
        DB().init()
    except err.MissingConfigurationError as ex:
        click.echo(str(ex))
        sys.exit(-1)
    os.makedirs(API_BASEDIR(), exist_ok=True)


# App
cli.add_command(install_application, name='install')
cli.add_command(uninstall_application, name='uninstall')


# Repository
cli.add_command(list_repository, name='repository')


# Runs
cli.add_command(runscli)


# Users
cli.add_command(register_user, name='register')


# Workflows
cli.add_command(workflowcli)
cli.add_command(run_workflow, name='run')
