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

from flowserv.config import env, FLOWSERV_API_BASEDIR
from flowserv.model.database import DB

import flowserv.error as err


@click.command()
def configuration():
    """Print configuration variables for flowServ."""
    envvar = 'export {}={}'
    click.echo('Current environment configuration settings:\n')
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
    # Create a new instance of the database
    config = env()
    basedir = config.get(FLOWSERV_API_BASEDIR)
    if basedir is None:
        raise err.MissingConfigurationError('base directory')
    os.makedirs(basedir, exist_ok=True)
    DB(config=config).init()
