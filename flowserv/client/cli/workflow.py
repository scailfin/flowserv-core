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

from flowserv.client.api import service

import flowserv.config.client as config
import flowserv.error as err
import flowserv.view.workflow as labels


# -- Create workflow ----------------------------------------------------------

@click.command()
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
def create_workflow(
    name, description, instructions, specfile, manifest, template,
    ignore_postproc
):
    """Create a new workflow for a given template."""
    with service() as api:
        # The create_workflow() method is only supported by the local API. If
        # an attempte is made to create a new workflow via a remote API an
        # error will be raised.
        doc = api.workflows().create_workflow(
            source=template,
            name=name,
            description=description,
            instructions=read_instructions(instructions),
            specfile=specfile,
            manifestfile=manifest,
            ignore_postproc=ignore_postproc
        )
    workflow_id = doc[labels.WORKFLOW_ID]
    click.echo('export {}={}'.format(config.ROB_BENCHMARK, workflow_id))


# -- Delete Workflow ----------------------------------------------------------

@click.command()
@click.argument('identifier')
def delete_workflow(identifier):
    """Delete an existing workflow and all runs."""
    with service() as api:
        api.workflows().delete_workflow(workflow_id=identifier)
    click.echo('workflow {} deleted.'.format(identifier))


# -- List workflows -----------------------------------------------------------

@click.command()
def list_workflows():
    """List all workflows."""
    count = 0
    with service() as api:
        for wf in api.workflows().list_workflows()[labels.WORKFLOW_LIST]:
            if count != 0:
                click.echo()
            count += 1
            title = 'Workflow {}'.format(count)
            click.echo(title)
            click.echo('-' * len(title))
            click.echo()
            click.echo('ID          : {}'.format(wf[labels.WORKFLOW_ID]))
            click.echo('Name        : {}'.format(wf[labels.WORKFLOW_NAME]))
            click.echo('Description : {}'.format(wf.get(labels.WORKFLOW_DESCRIPTION)))
            click.echo('Instructions: {}'.format(wf.get(labels.WORKFLOW_INSTRUCTIONS)))


# -- Update workflow ----------------------------------------------------------

@click.command()
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
def cli_workflow():
    """Create, delete, and maintain workflow templates in the repository."""
    pass


cli_workflow.add_command(create_workflow, name='create')
cli_workflow.add_command(delete_workflow, name='delete')
cli_workflow.add_command(list_workflows, name='list')
cli_workflow.add_command(update_workflow, name='update')


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
