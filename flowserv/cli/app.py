# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the flowServ app."""

import click

from flowserv.app import install_app, list_apps, uninstall_app
from flowserv.cli.repository import list_repository
from flowserv.model.database import DB

import flowserv.config.app as config


# -- Command group ------------------------------------------------------------

@click.group()
def cli():
    """Command line interface for administrative tasks."""
    pass


# -- CLI commands -------------------------------------------------------------

@cli.command(name='install')
@click.option(
    '-c', '--initdb',
    is_flag=True,
    default=False,
    help='Create a fresh database.'
)
@click.option(
    '-n', '--name',
    required=False,
    help='Application title.'
)
@click.option(
    '-d', '--description',
    required=False,
    help='Application sub-title.'
)
@click.option(
    '-i', '--instructions',
    type=click.Path(exists=False),
    required=False,
    help='File containing detailed instructions.'
)
@click.option(
    '-t', '--specfile',
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=False,
    help='Optional path to workflow specification file.'
)
@click.option(
    '-m', '--manifest',
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=False,
    help='Optional path to workflow manifest file.'
)
@click.argument('template')
def install_workflow(
    initdb, name, description, instructions, specfile, manifest, template
):
    """Install application from local folder or repository."""
    if initdb:
        click.echo('This will erase an existing database.')
        click.confirm('Continue?', default=True, abort=True)
        # Create a new instance of the database
        DB().init()
    # Install the application from the given workflow template.
    app_key = install_app(
        source=template,
        name=name,
        description=description,
        instructions=instructions,
        specfile=specfile,
        manifestfile=manifest
    )
    click.echo('export {}={}'.format(config.FLOWSERV_APP, app_key))


@cli.command(name='list')
def list_applications():
    """Listing of installed applications."""
    for name, key in list_apps():
        click.echo('{}\t{}'.format(key, name))


@cli.command('uninstall')
@click.argument('appkey')
def uninstall_application(appkey):
    """Uninstall application with the given key."""
    uninstall_app(app_key=appkey)


cli.add_command(list_repository, name='repository')
