# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to register a new user."""

import click
import sys

from flowserv.service.api import service

import flowserv.error as err


@click.command()
@click.option(
    '-u', '--username',
    required=True,
    prompt=True,
    help='User name'
)
@click.option(
    '-p', '--password',
    prompt=True,
    hide_input=True,
    confirmation_prompt=True,
    help='User password'
)
def register_user(username, password):
    """Register a new user."""
    try:
        with service() as api:
            api.users().register_user(
                username=username,
                password=password,
                verify=False
            )
    except err.DuplicateUserError as ex:
        click.echo(str(ex))
        sys.exit(-1)
    click.echo("user '{}' created".format(username))
