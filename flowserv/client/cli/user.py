# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to register a new user."""

import click

from flowserv.client.api import service
from flowserv.model.parameter.base import PARA_STRING
from flowserv.client.cli.table import ResultTable

import flowserv.view.user as labels


# -- List users ---------------------------------------------------------------

@click.command(name='users')
def list_users():
    """List all registered users."""
    with service() as api:
        doc = api.users().list_users()
    table = ResultTable(['Name', 'ID'], [PARA_STRING, PARA_STRING])
    for user in doc[labels.USER_LIST]:
        table.add([user[labels.USER_NAME], user[labels.USER_ID]])
    for line in table.format():
        click.echo(line)


# -- Login --------------------------------------------------------------------

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
    confirmation_prompt=False,
    help='User password'
)
@click.pass_context
def login_user(ctx, username, password):
    """Login to to obtain access token."""
    with service() as api:
        doc = api.users().login_user(username=username, password=password)
    # Get the access token from the response and print it to the console.
    token = doc[labels.USER_TOKEN]
    click.echo('export {}={}'.format(ctx.obj.vars['token'], token))


# -- Logout -------------------------------------------------------------------

@click.command()
@click.pass_context
def logout_user(ctx):
    """Logout from current user session."""
    with service() as api:
        api.users().logout_user(ctx.obj.access_token())
    click.echo('See ya mate!')


# -- Register -----------------------------------------------------------------

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
    with service() as api:
        doc = api.users().register_user(
            username=username,
            password=password,
            verify=False
        )
    user_id = doc[labels.USER_ID]
    click.echo('Registered {} with ID {}.'.format(username, user_id))


# -- Who am I -----------------------------------------------------------------

@click.command()
@click.pass_context
def whoami_user(ctx):
    """Print name of current user."""
    with service() as api:
        doc = api.users().whoami_user(ctx.obj.access_token())
    click.echo('Logged in as {}.'.format(doc[labels.USER_NAME]))


# -- Command group ------------------------------------------------------------

@click.group()
def cli_user():
    """Manage registered users."""
    pass


cli_user.add_command(list_users, name='list')
cli_user.add_command(register_user, name='register')
