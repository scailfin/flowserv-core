# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to manage workflow runs."""

import click

from flowserv.client.api import service
from flowserv.client.cli.parameter import read
from flowserv.client.cli.table import ResultTable
from flowserv.model.parameter.base import PARA_STRING
from flowserv.model.template.parameter import ParameterIndex
from flowserv.service.run.argument import deserialize_fh, serialize_arg

import flowserv.view.files as flbls
import flowserv.view.group as glbls
import flowserv.view.run as labels


# -- Cancel run ---------------------------------------------------------------

@click.command()
@click.argument('run')
def cancel_run(run):
    """Cancel active run."""
    with service() as api:
        doc = api.runs().cancel_run(run_id=run)
    click.echo('run {} canceled.'.format(doc[labels.RUN_ID]))


# -- Delete run ---------------------------------------------------------------

@click.command()
@click.argument('run')
def delete_run(run):
    """Delete run."""
    with service() as api:
        api.runs().delete_run(run_id=run)
    click.echo('run {} deleted.'.format(run))


# -- Download result files ----------------------------------------------------

@click.command()
@click.option(
    '-o', '--output',
    type=click.Path(writable=True),
    required=True,
    help='Save as ...'
)
@click.argument('run')
def download_result_archive(output, run):
    """Download archive of run result files."""
    with service() as api:
        buf = api.runs().get_result_archive(run_id=run).open()
        with open(output, 'wb') as local_file:
            local_file.write(buf.read())


@click.command()
@click.option('-f', '--file', required=True, help='File identifier')
@click.option(
    '-o', '--output',
    type=click.Path(writable=True),
    required=True,
    help='Save as ...'
)
@click.argument('run')
def download_result_file(file, output, run):
    """Download a run result file."""
    with service() as api:
        buf = api.runs().get_result_file(run_id=run, file_id=file).open()
        with open(output, 'wb') as local_file:
            local_file.write(buf.read())


# -- List runs ----------------------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.option(
    '-s', '--state',
    required=False,
    help='Run state filter'
)
@click.pass_context
def list_runs(ctx, group, state):
    """List workflow runs."""
    group_id = ctx.obj.get_group(ctx.params)
    with service() as api:
        doc = api.runs().list_runs(group_id=group_id, state=state)
    table = ResultTable(
        headline=['ID', 'Submitted at', 'State'],
        types=[PARA_STRING] * 3
    )
    for r in doc[labels.RUN_LIST]:
        run = list([r[labels.RUN_ID], r[labels.RUN_CREATED][:19], r[labels.RUN_STATE]])
        table.add(run)
    for line in table.format():
        click.echo(line)


# -- Show run information -----------------------------------------------------

@click.command()
@click.argument('run')
def show_run(run):
    """Show workflow run information."""
    with service() as api:
        doc = api.runs().get_run(run_id=run)
    click.echo('ID: {}'.format(doc[labels.RUN_ID]))
    if labels.RUN_STARTED in doc:
        click.echo('Started at: {}'.format(doc[labels.RUN_STARTED][:19]))
    if labels.RUN_FINISHED in doc:
        click.echo('Finished at: {}'.format(doc[labels.RUN_FINISHED][:19]))
    click.echo('State: {}'.format(doc[labels.RUN_STATE]))
    # Get index of parameters. The index contains the parameter name
    # and type
    parameters = ParameterIndex.from_dict(doc[labels.RUN_PARAMETERS])
    click.echo('\nArguments:')
    for arg in doc['arguments']:
        para = parameters[arg['name']]
        if para.is_file():
            file_id, target_path = deserialize_fh(arg['value'])
            value = '{} ({})'.format(file_id, target_path)
        else:
            value = arg['value']
        click.echo('  {} = {}'.format(para.name, value))
    if labels.RUN_ERRORS in doc:
        click.echo('\nMessages:')
        for msg in doc[labels.RUN_ERRORS]:
            click.echo('  {}'.format(msg))
    elif labels.RUN_FILES in doc:
        click.echo('\nFiles:')
        for res in doc[labels.RUN_FILES]:
            click.echo('  {} ({})'.format(res[flbls.FILE_ID], res[flbls.FILE_NAME]))


# -- Start new submission run -------------------------------------------------

@click.command()
@click.option(
    '-g', '--group',
    required=False,
    help='Group identifier'
)
@click.pass_context
def start_run(ctx, group):
    """Start new workflow run."""
    group_id = ctx.obj.get_group(ctx.params)
    with service() as api:
        doc = api.groups().get_group(group_id=group_id)
        # Create list of file descriptors for uploaded files that are included
        # in the submission handle
        files = []
        for fh in doc[glbls.GROUP_UPLOADS]:
            files.append((
                fh[flbls.FILE_ID],
                fh[flbls.FILE_NAME],
                fh[flbls.FILE_DATE][:19])
            )
        # Create list of additional user-provided template parameters
        parameters = ParameterIndex.from_dict(doc[glbls.GROUP_PARAMETERS])
        # Read values for all parameters.
        user_input = read(parameters.sorted(), files=files)
        args = [serialize_arg(key, val) for key, val in user_input.items()]
        # Start the run and print returned run state information.
        doc = api.runs().start_run(group_id=group_id, arguments=args)
        run_id = doc[labels.RUN_ID]
        run_state = doc[labels.RUN_STATE]
        click.echo('started run {} is {}'.format(run_id, run_state))


# -- Command Group ------------------------------------------------------------

@click.group(name='runs')
def cli_run():
    """Manage workflow runs."""
    pass


@click.group(name='download')
def cli_run_download():
    """Download run files."""
    pass


cli_run_download.add_command(download_result_archive, name='archive')
cli_run_download.add_command(download_result_file, name='file')

cli_run.add_command(cancel_run, name='cancel')
cli_run.add_command(delete_run, name='delete')
cli_run.add_command(cli_run_download)
cli_run.add_command(list_runs, name='list')
cli_run.add_command(show_run, name='show')
cli_run.add_command(start_run, name='start')
