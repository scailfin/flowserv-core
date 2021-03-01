# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to interact with workflow user groups."""

import click

from flowserv.client.api import service
from flowserv.client.cli.table import ResultTable
from flowserv.model.parameter.base import PARA_INT, PARA_STRING

import flowserv.util as util
import flowserv.view.files as filelabels
import flowserv.view.group as labels


# -- Create new user group ----------------------------------------------------

@click.command()
@click.option('-w', '--workflow', required=False, help='Workflow identifier')
@click.option('-n', '--name', required=True, help='Group name')
@click.option('-m', '--members', required=False, help='Group members')
@click.option(
    '-c', '--configfile',
    type=click.Path(exists=True),
    required=False,
    help='Group identifier'
)
@click.pass_context
def create_group(ctx, workflow, name, members, configfile):
    """Create a new user group."""
    workflow_id = ctx.obj.get_workflow(ctx.params)
    config = util.read_object(configfile) if configfile else None
    with service() as api:
        doc = api.groups().create_group(
            workflow_id=workflow_id,
            name=name,
            members=members.split(',') if members is not None else None,
            engine_config=config
        )
    group_id = doc[labels.GROUP_ID]
    click.echo('export {}={}'.format(ctx.obj.vars['group'], group_id))


# -- Delete user group --------------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.option(
    '-f', '--force',
    is_flag=True,
    default=False,
    help='Delete group without confirmation'
)
@click.pass_context
def delete_group(ctx, group, force):
    """Delete an existing user group."""
    group_id = ctx.obj.get_group(ctx.params)
    if not force:  # pragma: no cover
        msg = 'Do you really want to delete the group {}'.format(group_id)
        click.confirm(msg, default=True, abort=True)
    with service() as api:
        api.groups().delete_group(group_id)
    click.echo("Submission '{}' deleted.".format(group_id))


# -- List user groups ---------------------------------------------------------

@click.command()
def list_groups():
    """List user groups (for current user)."""
    with service() as api:
        doc = api.groups().list_groups()
    # Print listing of groups as output table.
    table = ResultTable(['ID', 'Name'], [PARA_STRING] * 2)
    for g in doc[labels.GROUP_LIST]:
        table.add([g[labels.GROUP_ID], g[labels.GROUP_NAME]])
    for line in table.format():
        click.echo(line)


# -- Show user group ----------------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.pass_context
def show_group(ctx, group):
    """Show user group information."""
    group_id = ctx.obj.get_group(ctx.params)
    with service() as api:
        doc = api.groups().get_group(group_id)
    print_group(doc)


# -- Update user group --------------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.option('-n', '--name', required=False, help='Group name')
@click.option('-m', '--members', required=False, help='Group members')
@click.pass_context
def update_group(ctx, group, name, members):
    """Update user group."""
    if name is None and members is None:
        raise click.UsageError('nothing to update')
    group_id = ctx.obj.get_group(ctx.params)
    with service() as api:
        doc = api.groups().update_group(
            group_id=group_id,
            name=name,
            members=members.split(',') if members is not None else None
        )
    print_group(doc)


# -- Command group ------------------------------------------------------------

@click.group()
def cli_group():
    """Create, modify, query and delete user groups."""
    pass


cli_group.add_command(create_group, name='create')
cli_group.add_command(delete_group, name='delete')
cli_group.add_command(list_groups, name='list')
cli_group.add_command(show_group, name='show')
cli_group.add_command(update_group, name='update')


# -- Helper Methods -----------------------------------------------------------

def print_group(doc):
    """Print group handle information to console.

    Parameters
    ----------
    doc: dict
        Serialization of a workflow group handle.
    """
    members = list()
    for u in doc[labels.GROUP_MEMBERS]:
        members.append(u[labels.USER_NAME])
    click.echo('ID      : {}'.format(doc[labels.GROUP_ID]))
    click.echo('Name    : {}'.format(doc[labels.GROUP_NAME]))
    click.echo('Members : {}'.format(','.join(members)))
    # -- Uploaded files -----------------------------------------------
    click.echo('\nUploaded Files\n--------------\n')
    table = ResultTable(
        headline=['ID', 'Name', 'Created At', 'Size'],
        types=[PARA_STRING, PARA_STRING, PARA_STRING, PARA_INT]
    )
    for f in doc[labels.GROUP_UPLOADS]:
        table.add([
            f[filelabels.FILE_ID],
            f[filelabels.FILE_NAME],
            f[filelabels.FILE_DATE][:19],
            f[filelabels.FILE_SIZE]
        ])
    for line in table.format():
        click.echo(line)
