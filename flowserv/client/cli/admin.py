# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the administrative tasks to configure the
environment, and the intialize the underlying database. These commands
operate locally.
"""

import click
import os
import sys

from flowserv.client.cli.config import get_configuration
from flowserv.config.api import API_BASEDIR
from flowserv.model.database import DB

import flowserv.error as err


@click.command()
def configuration():
    """Print configuration variables for flowServ."""
    comment = '\n#\n# {}\n#\n'
    envvar = 'export {}={}'
    for title, envs in get_configuration().items():
        click.echo(comment.format(title))
        for var, val in envs.items():
            click.echo(envvar.format(var, val))
    click.echo()


@click.command()
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
