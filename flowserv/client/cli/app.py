# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the flowServ app."""

import click

from flowserv.client.app.base import Flowserv


@click.command()
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
@click.pass_context
def install_application(
    ctx, key, name, description, instructions, specfile, manifest, template,
    ignore_postproc
):
    """Install workflow from local folder or repository."""
    # Install the application from the given workflow template.
    # Create a new workflow for the application from the specified template.
    app_key = Flowserv(open_access=True).install(
        source=template,
        identifier=key,
        name=name,
        description=description,
        instructions=instructions,
        specfile=specfile,
        manifestfile=manifest,
        ignore_postproc=ignore_postproc
    )
    click.echo('export {}={}'.format(ctx.obj.vars['workflow'], app_key))


@click.command()
@click.option(
    '-f', '--force',
    is_flag=True,
    default=False,
    help='Delete application without confirmation'
)
@click.argument('appkey')
def uninstall_application(force, appkey):
    """Uninstall workflow with the given key."""
    if not force:  # pragma: no cover
        click.echo('This will erase all workflow files and run results.')
        click.confirm('Continue?', default=True, abort=True)
    Flowserv(open_access=True).uninstall(identifier=appkey)


# -- Command group ------------------------------------------------------------

@click.group()
def cli_app():
    """Install and uninstall applications."""
    pass


cli_app.add_command(install_application, name='install')
cli_app.add_command(uninstall_application, name='uninstall')
