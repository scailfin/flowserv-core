# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the administrative tasks to configure the
environment, and the intialize the underlying database. These commands
operate locally.
"""

import click

from flowserv.config import env, FLOWSERV_DB
from flowserv.model.database import DB

import flowserv.error as err


@click.command()
def configuration():
    """Print configuration variables for flowServ."""
    envvar = 'export {}={}'
    click.echo('Configuration settings:\n')
    for var, value in env().items():
        click.echo(envvar.format(var, value))
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
    # Create a new instance of the database. Raise errors if the database URL
    # is not set.
    config = env()
    connect_url = config.get(FLOWSERV_DB)
    if connect_url is None:
        raise err.MissingConfigurationError('database Url')
    DB(connect_url=connect_url).init()
