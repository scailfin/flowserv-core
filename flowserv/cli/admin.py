# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the administrative tasks to configure the
environment, intialize the underlying database, and the create and maintain
workflows in the repository.
"""

import click

from flowserv.config.install import DB
from flowserv.cli.workflow import workflowcli

import flowserv.cli.config as config
import flowserv.core.error as err
import flowserv.core.util as util


@click.group()
def cli():
    """Command line interface for administrative tasks to manage a flowServ
    instance.
    """
    pass


@cli.command()
@click.option(
    '-d', '--dir',
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help='Base directory for API files (overrides FLOWSERV_API_DIR).'
)
@click.option(
    '-f', '--force',
    is_flag=True,
    default=False,
    help='Create database without confirmation'
)
def init(dir=None, force=False):
    """Initialize database and base directories for the flowServ API. The
    configuration parameters for the database are taken from the respective
    environment variables. Creates the API base directory if it does not exist.
    """
    if not force:
        click.echo('This will erase an existing database.')
        click.confirm('Continue?', default=True, abort=True)
    # Create a new instance of the database
    try:
        DB.init()
    except err.MissingConfigurationError as ex:
        click.echo(str(ex))
    # If the base directory is given ensure that the directory exists
    if dir is not None:
        base_dir = dir
    else:
        base_dir = config.API_BASEDIR()
    if base_dir is not None:
        util.create_dir(base_dir)


# Workflows
cli.add_command(workflowcli)
