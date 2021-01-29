# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to interact with group upload files."""

import click
import os

from flowserv.client.api import service
from flowserv.client.cli.table import ResultTable
from flowserv.model.files.fs import FSFile
from flowserv.model.parameter.base import PARA_INT, PARA_STRING

import flowserv.view.files as labels


# -- Delete file --------------------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.option('-f', '--file', required=True, help='File identifier')
@click.option(
    '--force',
    is_flag=True,
    default=False,
    help='Delete file without confirmation'
)
@click.pass_context
def delete_file(ctx, group, file, force):
    """Delete a previously uploaded file."""
    group_id = ctx.obj.get_group(ctx.params)
    if not force:  # pragma: no cover
        msg = 'Do you really want to delete file {}'
        click.confirm(msg, default=True, abort=True)
    with service() as api:
        api.uploads().delete_file(group_id=group_id, file_id=file)
    click.echo("File '{}' deleted.".format(file))


# -- Download file ------------------------------------------------------------

@click.command(name='download')
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.option('-f', '--file', required=True, help='File identifier')
@click.option(
    '-o', '--output',
    type=click.Path(writable=True),
    required=True,
    help='Save as ...'
)
@click.pass_context
def download_file(ctx, group, file, output):
    """Download a previously uploaded file."""
    group_id = ctx.obj.get_group(ctx.params)
    with service() as api:
        buf = api.uploads().get_uploaded_file(group_id=group_id, file_id=file)
        with open(output, 'wb') as local_file:
            local_file.write(buf.read())


# -- List files ---------------------------------------------------------------

@click.command(name='list')
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.pass_context
def list_files(ctx, group):
    """List uploaded files for a submission."""
    group_id = ctx.obj.get_group(ctx.params)
    with service() as api:
        doc = api.uploads().list_uploaded_files(group_id)
    table = ResultTable(
        headline=['ID', 'Name', 'Created At', 'Size'],
        types=[PARA_STRING, PARA_STRING, PARA_STRING, PARA_INT]
    )
    for f in doc['files']:
        table.add([
            f[labels.FILE_ID],
            f[labels.FILE_NAME],
            f[labels.FILE_DATE][:19],
            f[labels.FILE_SIZE]
        ])
    for line in table.format():
        click.echo(line)


# -- Upload file --------------------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.option(
    '-i', '--input',
    type=click.Path(exists=True, readable=True),
    required=True,
    help='Input file'
)
@click.pass_context
def upload_file(ctx, group, input):
    """Upload a file for a submission."""
    group_id = ctx.obj.get_group(ctx.params)
    filename = os.path.basename(input)
    with service() as api:
        doc = api.uploads().upload_file(
            group_id=group_id,
            file=FSFile(input),
            name=filename
        )
    file_id = doc[labels.FILE_ID]
    name = doc[labels.FILE_NAME]
    click.echo('Uploaded \'{}\' with ID {}.'.format(name, file_id))


# -- Command group ------------------------------------------------------------

@click.group()
def cli_uploads():
    """Upload, download, list and delete user files."""
    pass


cli_uploads.add_command(delete_file, name='delete')
cli_uploads.add_command(download_file, name='download')
cli_uploads.add_command(list_files, name='list')
cli_uploads.add_command(upload_file, name='upload')
