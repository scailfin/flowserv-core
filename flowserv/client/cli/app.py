# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the flowServ app."""

import click

from flowserv.app.base import install_app, uninstall_app

import flowserv.config.app as config


@click.command(name='install')
@click.option(
    '-k', '--key',
    required=False,
    help='Workflow application key.'
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
    '-s', '--specfile',
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
@click.option(
    '-g', '--ignore_postproc',
    is_flag=True,
    default=False,
    help='Print run logs'
)
@click.argument('template')
def install_application(
    key, name, description, instructions, specfile, manifest, template,
    ignore_postproc
):
    """Install workflow from local folder or repository."""
    # Install the application from the given workflow template.
    app_key = install_app(
        source=template,
        identifier=key,
        name=name,
        description=description,
        instructions=instructions,
        specfile=specfile,
        manifestfile=manifest,
        ignore_postproc=ignore_postproc
    )
    click.echo('export {}={}'.format(config.FLOWSERV_APP, app_key))


@click.command('uninstall')
@click.argument('appkey')
def uninstall_application(appkey):
    """Uninstall workflow with the given key."""
    uninstall_app(app_key=appkey)
