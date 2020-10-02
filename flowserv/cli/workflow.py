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
import logging
import os
import sys

from flowserv.app.base import App
from flowserv.cli.parameter import read
from flowserv.model.auth import open_access
from flowserv.service.api import service

import flowserv.error as err


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


# -- Run Workflow -------------------------------------------------------------

@click.option(
    '-o', '--output',
    type=click.Path(exists=False, file_okay=False, readable=True),
    required=False,
    help='Directory for output files.'
)
@click.option(
    '-v', '--verbose',
    is_flag=True,
    default=False,
    help='Print run logs'
)
@click.command(name='run')
@click.argument('identifier')
def run_workflow(identifier, output=None, verbose=False):
    """Run a workflow."""
    # -- Logging --------------------------------------------------------------
    if verbose:
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)
    # -- Setup ----------------------------------------------------------------
    app = App(key=identifier, auth=open_access)
    # -- Read input parameter values ------------------------------------------
    params = app.parameters().sorted()
    click.echo('\nWorkflow inputs\n---------------')
    args = read(params)
    # -- Start workflow run ---------------------------------------------------
    click.echo('\nStart Workflow\n--------------')
    run = app.start_run(arguments=args, poll_interval=1)
    click.echo('Run finished with {}'.format(run))
    # -- Run results ----------------------------------------------------------
    if run.is_error():
        for msg in run.messages():
            click.echo(msg)
    else:
        click.echo('\nRun files\n---------')
        for _, key, _ in run.files():
            click.echo(key)
            if output:
                out_file = os.path.join(output, key)
                run.get_file(key).store(out_file)
        postrun = app.get_postproc_results()
        if postrun is not None:
            click.echo('\nPost-Processing finished with {}'.format(postrun))
            if postrun.is_error():
                for msg in postrun.messages():
                    click.echo(msg)
            else:
                click.echo('\nPost-Processing files\n---------------------')
                for _, key, _ in postrun.files():
                    click.echo(key)
                    if output:
                        out_file = os.path.join(output, key)
                        postrun.get_file(key).store(out_file)


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
