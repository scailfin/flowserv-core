# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Administrator command line interface to create, delete and maintain
workflow templates in the repository.
"""

import click

from flowserv.client.api import service
from flowserv.client.cli.table import ResultTable
from flowserv.model.parameter.base import PARA_INT, PARA_STRING
from flowserv.model.workflow.manifest import read_instructions

import flowserv.view.files as flbls
import flowserv.view.run as rlbls
import flowserv.view.workflow as labels

import flowserv.util as util


# -- Create workflow ----------------------------------------------------------

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
    '-c', '--configfile',
    type=click.Path(exists=True),
    required=False,
    help='Group identifier'
)
@click.option(
    '-g', '--ignore_postproc',
    is_flag=True,
    default=False,
    help='Print run logs'
)
@click.option(
    '-v', '--verbose',
    is_flag=True,
    default=False,
    help='Print information about copied files'
)
@click.argument('template')
@click.pass_context
def create_workflow(
    ctx, key, name, description, instructions, specfile, manifest, template,
    configfile, ignore_postproc, verbose
):
    """Create a new workflow for a given template."""
    config = util.read_object(configfile) if configfile else None
    with service() as api:
        # The create_workflow() method is only supported by the local API. If
        # an attempte is made to create a new workflow via a remote API an
        # error will be raised.
        doc = api.workflows().create_workflow(
            source=template,
            identifier=key,
            name=name,
            description=description,
            instructions=read_instructions(instructions),
            specfile=specfile,
            manifestfile=manifest,
            engine_config=config,
            ignore_postproc=ignore_postproc,
            verbose=verbose
        )
    workflow_id = doc[labels.WORKFLOW_ID]
    click.echo('export {}={}'.format(ctx.obj.vars['workflow'], workflow_id))


# -- Delete Workflow ----------------------------------------------------------

@click.command()
@click.option('-w', '--workflow', required=False, help='Workflow identifier')
@click.pass_context
def delete_workflow(ctx, workflow):
    """Delete an existing workflow and all runs."""
    workflow_id = ctx.obj.get_workflow(ctx.params)
    with service() as api:
        api.workflows().delete_workflow(workflow_id=workflow_id)
    click.echo('workflow {} deleted.'.format(workflow_id))


# -- Download result files ----------------------------------------------------

@click.command()
@click.option(
    '-o', '--output',
    type=click.Path(writable=True),
    required=True,
    help='Save as ...'
)
@click.option('-w', '--workflow', required=False, help='Workflow identifier')
@click.pass_context
def download_result_archive(ctx, output, workflow):
    """Download post-processing result archive."""
    workflow_id = ctx.obj.get_workflow(ctx.params)
    if workflow_id is None:
        raise click.UsageError('no workflow specified')
    with service() as api:
        buf = api.workflows().get_result_archive(workflow_id=workflow_id).open()
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
@click.option('-w', '--workflow', required=False, help='Workflow identifier')
@click.pass_context
def download_result_file(ctx, file, output, workflow):
    """Download post-processing result file."""
    workflow_id = ctx.obj.get_workflow(ctx.params)
    if workflow_id is None:
        raise click.UsageError('no workflow specified')
    with service() as api:
        fh = api.workflows().get_result_file(workflow_id=workflow_id, file_id=file)
    buf = fh.open()
    with open(output, 'wb') as local_file:
        local_file.write(buf.read())


# -- Get workflow -------------------------------------------------------------

@click.command()
@click.option('-w', '--workflow', required=False, help='Workflow identifier')
@click.pass_context
def get_workflow(ctx, workflow):
    """Print workflow properties."""
    workflow_id = ctx.obj.get_workflow(ctx.params)
    with service() as api:
        doc = api.workflows().get_workflow(workflow_id=workflow_id)
    click.echo('ID          : {}'.format(doc[labels.WORKFLOW_ID]))
    click.echo('Name        : {}'.format(doc[labels.WORKFLOW_NAME]))
    click.echo('Description : {}'.format(doc.get(labels.WORKFLOW_DESCRIPTION)))
    click.echo('Instructions: {}'.format(doc.get(labels.WORKFLOW_INSTRUCTIONS)))
    if labels.POSTPROC_RUN in doc:
        postproc = doc[labels.POSTPROC_RUN]
        click.echo('\nPost-processing\n---------------')
        if rlbls.RUN_ERRORS in postproc:
            for msg in postproc[rlbls.RUN_ERRORS]:
                click.echo('{}'.format(msg))
        elif rlbls.RUN_FILES in postproc:
            for f in postproc[rlbls.RUN_FILES]:
                click.echo('{} ({})'.format(f[flbls.FILE_ID], f[flbls.FILE_NAME]))


# -- List workflows -----------------------------------------------------------

@click.command()
def list_workflows():
    """List all workflows."""
    count = 0
    with service() as api:
        doc = api.workflows().list_workflows()
    for wf in doc[labels.WORKFLOW_LIST]:
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


# -- Result ranking -----------------------------------------------------------

@click.command()
@click.option(
    '-a', '--all',
    is_flag=True,
    default=False,
    help='Include all runs.'
)
@click.option('-w', '--workflow', required=False, help='Workflow identifier')
@click.pass_context
def show_ranking(ctx, workflow, all):
    """Show ranking for workflow results."""
    workflow_id = ctx.obj.get_workflow(ctx.params)
    with service() as api:
        doc = api.workflows().get_ranking(workflow_id=workflow_id, include_all=all)
    # Print ranking.
    headline = ['Rank', 'Name']
    types = [PARA_INT, PARA_STRING]
    mapping = dict()
    for col in doc[labels.WORKFLOW_SCHEMA]:
        headline.append(col[labels.COLUMN_TITLE])
        types.append(col[labels.COLUMN_TYPE])
        mapping[col[labels.COLUMN_NAME]] = len(mapping)
    table = ResultTable(headline=headline, types=types)
    rank = 1
    for run in doc[labels.RANKING]:
        group = run[labels.WORKFLOW_GROUP][labels.GROUP_NAME]
        row = [rank, group] + ([None] * (len(headline) - 2))
        for r in run[labels.RUN_RESULTS]:
            row[mapping[r[labels.COLUMN_NAME]] + 2] = r[labels.COLUMN_VALUE]
        table.add(row)
        rank += 1
    for line in table.format():
        click.echo(line)


# -- Update workflow ----------------------------------------------------------

@click.command()
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
@click.option('-w', '--workflow', required=False, help='Workflow identifier')
@click.pass_context
def update_workflow(ctx, name, description, instructions, workflow):
    """Update workflow properties."""
    workflow_id = ctx.obj.get_workflow(ctx.params)
    # Ensure that at least one of the optional arguments is given
    if name is None and description is None and instructions is None:
        click.echo('nothing to update')
    else:
        with service() as api:
            api.workflows().update_workflow(
                workflow_id=workflow_id,
                name=name,
                description=description,
                instructions=read_instructions(instructions)
            )
        click.echo('updated workflow {}'.format(workflow_id))


# -- Command Group ------------------------------------------------------------

@click.group(name='workflows')
def cli_benchmark():
    """Show benchmark templates."""
    pass  # pragma: no cover


@click.group(name='workflows')
def cli_workflow():
    """Create, delete, and maintain workflow templates in the repository."""
    pass  # pragma: no cover


@click.group(name='download')
def cli_workflow_download():
    """Download post-processing results."""
    pass  # pragma: no cover


cli_workflow_download.add_command(download_result_archive, name='archive')
cli_workflow_download.add_command(download_result_file, name='file')

cli_workflow.add_command(create_workflow, name='create')
cli_workflow.add_command(delete_workflow, name='delete')
cli_workflow.add_command(cli_workflow_download)
cli_workflow.add_command(list_workflows, name='list')
cli_workflow.add_command(show_ranking, name='ranking')
cli_workflow.add_command(get_workflow, name='show')
cli_workflow.add_command(update_workflow, name='update')

cli_benchmark.add_command(cli_workflow_download)
cli_benchmark.add_command(list_workflows, name='list')
cli_benchmark.add_command(show_ranking, name='ranking')
cli_benchmark.add_command(get_workflow, name='show')
