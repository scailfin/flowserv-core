# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Administrator command line interface to create, delete and maintain
workflow templates in the repository.
"""

import click
import sys

from flowserv.service.api import service

import flowserv.error as err


# -- Add workflow -------------------------------------------------------------

@click.command(name='create')
@click.option(
    '-n', '--name',
    required=False,
    help='Unique workflow name.'
)
@click.option(
    '-d', '--description',
    required=False,
    help='Short workflow description.'
)
@click.option(
    '-i', '--instructions',
    type=click.Path(exists=False),
    required=False,
    help='File containing instructions for running the workflow.'
)
@click.option(
    '-f', '--specfile',
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
def add_workflow(
    name, description, instructions, specfile, manifest, template
):
    """Create a new workflow."""
    # Add workflow template to repository
    try:
        # Use the workflow service component to create the workflow. This
        # ensures that the result table is also created if the template
        # specifies a result schema.
        with service() as api:
            wf = api.workflows().create_workflow(
                source=template,
                name=name,
                description=description,
                instructions=read_instructions(instructions),
                specfile=specfile,
                manifestfile=manifest
            )
        click.echo('export FLOWSERV_WORKFLOW={}'.format(wf['id']))
    except (err.ConstraintViolationError, ValueError) as ex:
        click.echo(str(ex))
        sys.exit(-1)


# -- Delete workflow ----------------------------------------------------------

@click.command(name='delete')
@click.argument('identifier')
def delete_workflow(identifier):
    """Delete a given workflow."""
    try:
        with service() as api:
            api.workflows().delete_workflow(identifier)
        click.echo('deleted workflow {}'.format(identifier))
    except err.UnknownObjectError as ex:
        click.echo(str(ex))
        sys.exit(-1)


# -- List workflows -----------------------------------------------------------

@click.command(name='list')
def list_workflows():
    """List all workflows."""
    count = 0
    with service() as api:
        for wf in api.workflows().list_workflows()['workflows']:
            if count != 0:
                click.echo()
            count += 1
            title = 'Benchmark {}'.format(count)
            click.echo(title)
            click.echo('-' * len(title))
            click.echo()
            click.echo('ID          : {}'.format(wf['id']))
            click.echo('Name        : {}'.format(wf['name']))
            click.echo('Description : {}'.format(wf.get('description')))
            click.echo('Instructions: {}'.format(wf.get('instructions')))


# -- Update workflow ----------------------------------------------------------

@click.command(name='update')
@click.argument('identifier')
@click.option(
    '-n', '--name',
    required=False,
    help='Unique workflow name.'
)
@click.option(
    '-d', '--description',
    required=False,
    help='Short description.'
)
@click.option(
    '-i', '--instructions',
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=False,
    help='File containing instructions for participants.'
)
def update_workflow(
    identifier, name=None, description=None, instructions=None
):
    """Update workflow properties."""
    # Ensure that at least one of the optional arguments is given
    if name is None and description is None and instructions is None:
        click.echo('nothing to update')
    else:
        try:
            with service() as api:
                api.workflows().update_workflow(
                    workflow_id=identifier,
                    name=name,
                    description=description,
                    instructions=read_instructions(instructions)
                )
            click.echo('updated workflow {}'.format(identifier))
        except (err.UnknownObjectError, err.ConstraintViolationError) as ex:
            click.echo(str(ex))
            sys.exit(-1)


# -- Command Group ------------------------------------------------------------

@click.group(name='workflows')
def workflowcli():
    """Create, delete, and maintain workflow templates in the repository."""
    pass


workflowcli.add_command(add_workflow)
workflowcli.add_command(delete_workflow)
workflowcli.add_command(list_workflows)
workflowcli.add_command(update_workflow)


# -- Helper Methods -----------------------------------------------------------

def read_instructions(filename):
    """Read instruction text from a given file. If the filename is None the
    result will be None as well.

    Returns
    -------
    string
    """
    # Read instructions from file if given
    instruction_text = None
    if filename is not None:
        with open(filename, 'r') as f:
            instruction_text = f.read().strip()
    return instruction_text
