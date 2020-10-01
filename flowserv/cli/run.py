# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to manage workflow runs."""

import click
import sys

from flowserv.service.api import service

import flowserv.error as err


@click.command('delete')
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
    try:
        with service() as api:
            count = api.runs().run_manager.delete_obsolete_runs(
                date=before,
                state=state
            )
            click.echo('{} runs deleted.'.format(count))
    except err.DuplicateUserError as ex:
        click.echo(str(ex))
        sys.exit(-1)


@click.command('list')
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
    try:
        with service() as api:
            runs = api.runs().run_manager.list_obsolete_runs(
                date=before,
                state=state
            )
            for run in runs:
                click.echo('{}\t{}\t{}'.format(
                    run.run_id,
                    run.created_at,
                    run.state
                ))
    except err.DuplicateUserError as ex:
        click.echo(str(ex))
        sys.exit(-1)


# -- Command Group ------------------------------------------------------------

@click.group(name='runs')
def runscli():
    """Manage workflow runs."""
    pass


runscli.add_command(delete_obsolete_runs)
runscli.add_command(list_obsolete_runs)
