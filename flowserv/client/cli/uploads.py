# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
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

import flowserv.config.client as config
import flowserv.view.files as labels


# -- Delete file --------------------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.option('-f', '--file', required=True, help='File identifier')
def delete_file(group, file):
    """Delete a previously uploaded file."""
    group_id = group if group is not None else config.SUBMISSION_ID()
    if group_id is None:
        raise click.UsageError('no group identifier given')
    msg = 'Do you really want to delete file {}'
    if not click.confirm(msg.format(file)):
        return
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
def download_file(group, file, output):
    """Download a previously uploaded file."""
    group_id = group if group is not None else config.SUBMISSION_ID()
    if group_id is None:
        raise click.UsageError('no group identifier given')
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
def list_files(group):
    """List uploaded files for a submission."""
    group_id = group if group is not None else config.SUBMISSION_ID()
    if group_id is None:
        raise click.UsageError('no group identifier given')
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
def upload_file(group, input):
    """Upload a file for a submission."""
    group_id = group if group is not None else config.SUBMISSION_ID()
    if group_id is None:
        raise click.UsageError('no group identifier given')
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