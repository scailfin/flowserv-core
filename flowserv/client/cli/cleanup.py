# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to list and delete old workflow runs."""

import click

from flowserv.client.api import service
from flowserv.client.cli.table import ResultTable
from flowserv.model.parameter.base import PARA_STRING


@click.command()
@click.option(
    '-d', '--before',
    required=True,
    prompt=True,
    help='Run date filter'
)
@click.option(
    '-s', '--state',
    help='Run state filter'
)
def delete_obsolete_runs(before, state):
    """Delete old runs."""
    with service() as api:
        count = api.runs().run_manager.delete_obsolete_runs(
            date=before,
            state=state
        )
        click.echo('{} runs deleted.'.format(count))


@click.command()
@click.option(
    '-d', '--before',
    required=True,
    prompt=True,
    help='Run date filter'
)
@click.option(
    '-s', '--state',
    help='Run state filter'
)
def list_obsolete_runs(before, state):
    """List old runs."""
    table = ResultTable(headline=['ID', 'Submitted at', 'State'], types=[PARA_STRING] * 3)
    with service() as api:
        runs = api.runs().run_manager.list_obsolete_runs(
            date=before,
            state=state
        )
        for run in runs:
            table.add([run.run_id, run.created_at[:19], run.state_type])
    for line in table.format():
        click.echo(line)


# -- Command Group ------------------------------------------------------------

@click.group()
def cli_cleanup():
    """Manage workflow runs."""
    pass


cli_cleanup.add_command(delete_obsolete_runs, name='delete')
cli_cleanup.add_command(list_obsolete_runs, name='list')
