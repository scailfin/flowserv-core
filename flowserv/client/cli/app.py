# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the flowServ app."""

import click

from flowserv.client.api import service

import flowserv.config as config
import flowserv.view.workflow as labels


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
def install_application(
    key, name, description, instructions, specfile, manifest, template,
    ignore_postproc
):
    """Install workflow from local folder or repository."""
    # Install the application from the given workflow template.
    # Create a new workflow for the application from the specified template.
    with service(user_id=config.DEFAULT_USER) as api:
        doc = api.workflows().create_workflow(
            source=template,
            identifier=key,
            name=name,
            description=description,
            instructions=instructions,
            specfile=specfile,
            manifestfile=manifest,
            ignore_postproc=ignore_postproc
        )
        workflow_id = doc[labels.WORKFLOW_ID]
        api.groups().create_group(
            workflow_id=workflow_id,
            name=workflow_id,
            identifier=workflow_id
        )
    click.echo('export {}={}'.format(config.FLOWSERV_APP, app_key))


@click.command()
@click.argument('appkey')
def uninstall_application(appkey):
    """Uninstall workflow with the given key."""
    # Delete workflow and all related files.
    with service() as api:
        api.workflows.delete_workflow(workflow_id=appkey)


# -- Command group ------------------------------------------------------------

@click.group()
def cli_app():
    """Install and uninstall applications."""
    pass


cli_app.add_command(install_application, name='install')
cli_app.add_command(uninstall_application, name='uninstall')
