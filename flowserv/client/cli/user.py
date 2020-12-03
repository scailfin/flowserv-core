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

from flowserv.client.base import client

import flowserv.error as err


@click.group(name='user')
def cli_user():
    """Manage registered users."""
    pass


# -- List users ---------------------------------------------------------------

@click.command(name='users')
@click.pass_context
def list(ctx):
    """List all registered users."""
    url = ctx.obj['URLS'].list_users()
    headers = ctx.obj['HEADERS']
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        body = r.json()
        if ctx.obj['RAW']:
            click.echo(json.dumps(body, indent=4))
        else:
            table = ResultTable(['Name', 'ID'], [PARA_STRING, PARA_STRING])
            for user in body['users']:
                table.add([user['username'], user['id']])
            for line in table.format():
                click.echo(line)
    except (requests.ConnectionError, requests.HTTPError) as ex:
        click.echo('{}'.format(ex))


# -- Login --------------------------------------------------------------------

@click.command()
@click.pass_context
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
def login(ctx, username, password):
    """Login to to obtain access token."""
    url = ctx.obj['URLS'].login()
    headers = ctx.obj['HEADERS']
    data = {'username': username, 'password': password}
    try:
        r = requests.post(url, json=data, headers=headers)
        r.raise_for_status()
        body = r.json()
        if ctx.obj['RAW']:
            click.echo(json.dumps(body, indent=4))
        else:
            token = body['token']
            click.echo('export {}={}'.format(config.ROB_ACCESS_TOKEN, token))
    except (requests.ConnectionError, requests.HTTPError) as ex:
        click.echo('{}'.format(ex))


# -- Logout -------------------------------------------------------------------

@click.command()
@click.pass_context
def logout(ctx):
    """Logout from current user session."""
    # Get user info using the access token
    url = ctx.obj['URLS'].logout()
    headers = ctx.obj['HEADERS']
    try:
        r = requests.post(url, headers=headers)
        r.raise_for_status()
        body = r.json()
        if ctx.obj['RAW']:
            click.echo(json.dumps(body, indent=4))
        else:
            click.echo('See ya mate!')
    except (requests.ConnectionError, requests.HTTPError) as ex:
        click.echo('{}'.format(ex))


# -- Register -----------------------------------------------------------------

@click.command()
@click.pass_context
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
def register(ctx, username, password):
    """Register a new user."""
    url = ctx.obj['URLS'].register_user()
    headers = ctx.obj['HEADERS']
    data = {
        'username': username,
        'password': password,
        'verify': False
    }
    try:
        r = requests.post(url, json=data, headers=headers)
        r.raise_for_status()
        body = r.json()
        if ctx.obj['RAW']:
            click.echo(json.dumps(body, indent=4))
        else:
            user_id = body['id']
            user_name = body['username']
            click.echo('Registered {} with ID {}.'.format(user_name, user_id))
    except (requests.ConnectionError, requests.HTTPError) as ex:
        click.echo('{}'.format(ex))


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
        with client() as api:
            r = api.users().register_user(
                username=username,
                password=password,
                verify=False
            )
    except err.DuplicateUserError as ex:
        click.echo(str(ex))
        sys.exit(-1)
    click.echo("user '{}' created".format(username))


# -- Reset Password -----------------------------------------------------------

@click.command(name='pwd')
@click.pass_context
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
    help='New user password'
)
def reset_password(ctx, username, password):
    """Reset user password."""
    url = ctx.obj['URLS'].request_password_reset()
    headers = ctx.obj['HEADERS']
    data = {'username': username}
    try:
        r = requests.post(url, json=data, headers=headers)
        r.raise_for_status()
        body = r.json()
        reqest_id = body['requestId']
        url = ctx.obj['URLS'].reset_password()
        data = {'requestId': reqest_id, 'password': password}
        r = requests.post(url, json=data, headers=headers)
        r.raise_for_status()
        if ctx.obj['RAW']:
            click.echo(json.dumps(body, indent=4))
        else:
            click.echo('Password reset.')
    except (requests.ConnectionError, requests.HTTPError) as ex:
        click.echo('{}'.format(ex))


# -- Who am I -----------------------------------------------------------------

@click.command()
@click.pass_context
def whoami(ctx):
    """Print name of current user."""
    # Get user info using the access token
    try:
        r = requests.get(ctx.obj['URLS'].whoami(), headers=ctx.obj['HEADERS'])
        r.raise_for_status()
        body = r.json()
        if ctx.obj['RAW']:
            click.echo(json.dumps(body, indent=4))
        else:
            click.echo('Logged in as {}.'.format(body['username']))
    except (requests.ConnectionError, requests.HTTPError) as ex:
        click.echo('{}'.format(ex))


cli_user.add_command(register_user)
