# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the administrative tasks to configure the
environment, intialize the underlying database, and the create and maintain
workflows in the repository.
"""

import click
import os

from flowserv.config.auth import FLOWSERV_AUTH_LOGINTTL, AUTH_LOGINTTL
from flowserv.config.backend import (
    FLOWSERV_BACKEND_CLASS, FLOWSERV_BACKEND_MODULE)
from flowserv.config.install import DB
from flowserv.core.db.driver import DatabaseDriver
from flowserv.cli.workflow import workflowcli
from flowserv.service.backend import init_backend

import flowserv.config.api as api
import flowserv.core.error as err
import flowserv.core.util as util


@click.group()
def cli():
    """Command line interface for administrative tasks to manage a flowServ
    instance.
    """
    pass


@cli.command(name='config')
@click.option(
    '-a', '--all',
    is_flag=True,
    default=False,
    help='Show all configuration variables'
)
@click.option(
    '-d', '--database',
    is_flag=True,
    default=False,
    help='Show database configuration variables'
)
@click.option(
    '-u', '--auth',
    is_flag=True,
    default=False,
    help='Show database configuration variables'
)
@click.option(
    '-b', '--backend',
    is_flag=True,
    default=False,
    help='Show workflow controller configuration variables'
)
@click.option(
    '-s', '--service',
    is_flag=True,
    default=False,
    help='Show Web Service API configuration variables'
)
def configuration(all=False, database=False, auth=False, backend=False, service=False):
    """Print configuration variables for flowServ."""
    comment = '#\n# {}\n#'
    envvar = 'export {}={}'
    # Configuration for the API
    if service or all:
        click.echo(comment.format('Web Service API'))
        conf = list()
        conf.append((api.FLOWSERV_API_BASEDIR, api.API_BASEDIR()))
        conf.append((api.FLOWSERV_API_NAME, '"{}"'.format(api.API_NAME())))
        conf.append((api.FLOWSERV_API_HOST, api.API_HOST()))
        conf.append((api.FLOWSERV_API_PORT, api.API_PORT()))
        conf.append((api.FLOWSERV_API_PROTOCOL, api.API_PROTOCOL()))
        conf.append((api.FLOWSERV_API_PATH, api.API_PATH()))
        for var, val in conf:
            click.echo(envvar.format(var, val))
    # Configuration for user authentication
    if auth or all:
        click.echo(comment.format('Authentication'))
        conf = [(FLOWSERV_AUTH_LOGINTTL, AUTH_LOGINTTL())]
        for var, val in conf:
            click.echo(envvar.format(var, val))
    # Configuration for the underlying database
    if database or all:
        click.echo(comment.format('Database'))
        for var, val in DatabaseDriver.configuration():
            click.echo(envvar.format(var, val))
    # Configuration for thw workflow execution backend
    if backend or all:
        click.echo(comment.format('Workflow Controller'))
        conf = list()
        backend_class = os.environ.get(FLOWSERV_BACKEND_CLASS, '')
        conf.append((FLOWSERV_BACKEND_CLASS, backend_class))
        backend_module = os.environ.get(FLOWSERV_BACKEND_MODULE, '')
        conf.append((FLOWSERV_BACKEND_MODULE, backend_module))
        for var, val in init_backend(raise_error=False).configuration():
            click.echo(envvar.format(var, val))


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
        base_dir = api.API_BASEDIR()
    if base_dir is not None:
        util.create_dir(base_dir)


# Workflows
cli.add_command(workflowcli)
