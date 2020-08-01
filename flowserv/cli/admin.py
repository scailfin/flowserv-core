# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the administrative tasks to configure the
environment, intialize the underlying database, and the create and maintain
workflows in the repository.
"""

import click
import logging
import os
import sys
import tempfile

from flowserv.cli.parameter import read
from flowserv.cli.repository import list_repository
from flowserv.cli.user import register_user
from flowserv.cli.workflow import add_workflow, workflowcli
from flowserv.config.api import (
    API_BASEDIR, API_HOST, API_NAME, API_PATH, API_PORT, API_PROTOCOL,
    FLOWSERV_API_BASEDIR, FLOWSERV_API_HOST, FLOWSERV_API_NAME,
    FLOWSERV_API_PATH, FLOWSERV_API_PORT, FLOWSERV_API_PROTOCOL
)
from flowserv.config.auth import FLOWSERV_AUTH_LOGINTTL, AUTH_LOGINTTL
from flowserv.config.backend import (
    FLOWSERV_BACKEND_CLASS, FLOWSERV_BACKEND_MODULE
)
from flowserv.config.controller import FLOWSERV_ASYNC
from flowserv.config.database import FLOWSERV_DB, DB_CONNECT
from flowserv.model.database import DB, TEST_URL
from flowserv.model.parameter.files import InputFile
from flowserv.model.template.parameter import ParameterIndex
from flowserv.service.api import service
from flowserv.service.run.argument import ARG, FILE

import flowserv.error as err
import flowserv.util as util


@click.group()
def cli():
    """Command line interface for administrative tasks to manage a flowServ
    instance.
    """
    pass


@cli.command(name='config')
@click.option(
    '-a', '--all',
    is_flag=True,
    default=False,
    help='Show all configuration variables'
)
@click.option(
    '-d', '--database',
    is_flag=True,
    default=False,
    help='Show database configuration variables'
)
@click.option(
    '-u', '--auth',
    is_flag=True,
    default=False,
    help='Show database configuration variables'
)
@click.option(
    '-b', '--backend',
    is_flag=True,
    default=False,
    help='Show workflow controller configuration variables'
)
@click.option(
    '-s', '--service',
    is_flag=True,
    default=False,
    help='Show Web Service API configuration variables'
)
def configuration(
    all=False, database=False, auth=False, backend=False, service=False
):
    """Print configuration variables for flowServ."""
    # Show all configuration variables if no command line option is given:
    if not (all or database or auth or backend or service):
        all = True
    comment = '#\n# {}\n#'
    envvar = 'export {}={}'
    # Configuration for the API
    if service or all:
        click.echo(comment.format('Web Service API'))
        conf = list()
        conf.append((FLOWSERV_API_BASEDIR, API_BASEDIR()))
        conf.append((FLOWSERV_API_NAME, '"{}"'.format(API_NAME())))
        conf.append((FLOWSERV_API_HOST, API_HOST()))
        conf.append((FLOWSERV_API_PORT, API_PORT()))
        conf.append((FLOWSERV_API_PROTOCOL, API_PROTOCOL()))
        conf.append((FLOWSERV_API_PATH, API_PATH()))
        for var, val in conf:
            click.echo(envvar.format(var, val))
    # Configuration for user authentication
    if auth or all:
        click.echo(comment.format('Authentication'))
        conf = [(FLOWSERV_AUTH_LOGINTTL, AUTH_LOGINTTL())]
        for var, val in conf:
            click.echo(envvar.format(var, val))
    # Configuration for the underlying database
    if database or all:
        click.echo(comment.format('Database'))
        try:
            connect_url = DB_CONNECT()
        except err.MissingConfigurationError:
            connect_url = 'None'
        click.echo(envvar.format(FLOWSERV_DB, connect_url))
    # Configuration for the workflow execution backend
    if backend or all:
        from flowserv.service.backend import init_backend
        click.echo(comment.format('Workflow Controller'))
        conf = list()
        backend_class = os.environ.get(FLOWSERV_BACKEND_CLASS, '')
        conf.append((FLOWSERV_BACKEND_CLASS, backend_class))
        backend_module = os.environ.get(FLOWSERV_BACKEND_MODULE, '')
        conf.append((FLOWSERV_BACKEND_MODULE, backend_module))
        for var, val in conf:
            click.echo(envvar.format(var, val))
        for var, val in init_backend(raise_error=False).configuration():
            click.echo(envvar.format(var, val))


@cli.command()
@click.option(
    '-f', '--force',
    is_flag=True,
    default=False,
    help='Create database without confirmation'
)
def init(force=False):
    """Initialize database and base directories for the API."""
    if not force:
        click.echo('This will erase an existing database.')
        click.confirm('Continue?', default=True, abort=True)
    # Create a new instance of the database
    try:
        DB().init()
    except err.MissingConfigurationError as ex:
        click.echo(str(ex))
        sys.exit(-1)
    util.create_dir(API_BASEDIR())


# -- Run workflow template for testing purposes -------------------------------

@cli.command(name='run')
@click.option(
    '-f', '--specfile',
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
    '-i', '--ignorepp',
    is_flag=True,
    default=False,
    help='Ignore post-processing workflow'
)
@click.option(
    '-o', '--output',
    type=click.Path(exists=False, file_okay=False, readable=True),
    required=False,
    help='Directory for output files.'
)
@click.argument('template')
def run_workflow(specfile, manifest, ignorepp, output, source):  # pragma: no cover  # noqa: E501
    """Run a workflow template for test purposes."""
    # -- Logging --------------------------------------------------------------
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    # -- Setup ----------------------------------------------------------------
    tmpdir = tempfile.mkdtemp()
    os.environ[FLOWSERV_API_BASEDIR] = tmpdir
    os.environ[FLOWSERV_ASYNC] = 'False'
    os.environ[FLOWSERV_DB] = TEST_URL
    from flowserv.service.database import database
    database.init()
    # -- Add workflow template to repository ----------------------------------
    with service() as api:
        workflow = api.workflows().create_workflow(
            name=util.get_short_identifier(),
            source=source,
            specfile=specfile,
            manifest=manifest,
            ignore_postproc=ignorepp
        )
        workflow_id = workflow['id']
    # -- Create user ----------------------------------------------------------
    with service() as api:
        user_id = api.users().register_user(
            username='test',
            password=util.get_unique_identifier(),
            verify=False
        )['id']
    # -- Create a new submission for the workflow -----------------------------
    with service() as api:
        submission_id = api.groups().create_group(
            workflow_id=workflow_id,
            name='test',
            user_id=user_id
        )['id']
    # -- Read input parameter values ------------------------------------------
    params = ParameterIndex().from_dict(workflow['parameters']).sorted()
    click.echo('\nWorkflow inputs\n---------------')
    args = read(params)
    # -- Upload files ---------------------------------------------------------
    runargs = list()
    for key, value in args.items():
        if isinstance(value, InputFile):
            filename = value.source()
            target_path = value.target()
            with service() as api:
                file_id = api.uploads().upload_file(
                    group_id=submission_id,
                    file=filename,
                    name=os.path.basename(filename),
                    user_id=user_id
                )['id']
                value = FILE(file_id, target_path)
        runargs.append(ARG(key, value))
    # -- Start workflow run ---------------------------------------------------
    click.echo('\nStart Workflow\n--------------')
    with service() as api:
        run = api.runs().start_run(
            group_id=submission_id,
            arguments=runargs,
            user_id=user_id
        )
    runout = os.path.join(output, 'run') if output is not None else None
    click.echo('\nRun results\n-----------')
    run_results(run, user_id, runout)
    # -- Get post-processing results ------------------------------------------
    with service() as api:
        workflow = api.workflows().get_workflow(workflow_id)
    postout = os.path.join(output, 'postproc') if output is not None else None
    postrun = workflow.get('postproc')
    if postrun is not None:
        click.echo('\nPost-processing results\n-----------------------')
        run_results(postrun, user_id, postout)


# Repository
cli.add_command(list_repository, name='repository')


# Users
cli.add_command(register_user, name='register')


# Workflows
cli.add_command(workflowcli)
cli.add_command(add_workflow, name='install')


# -- Helper methods -----------------------------------------------------------

def run_results(run, user_id, output):
    run_id = run['id']
    click.echo('\nRun finished with {}'.format(run['state']))
    if run['state'] == 'ERROR':
        for msg in run['messages']:
            click.echo(msg)
    elif run['state'] == 'SUCCESS':
        # Get index of output files.
        files = dict()
        for obj in run['files']:
            with service() as api:
                file_id = obj['id']
                fh = api.runs().get_result_file(
                    run_id=run_id,
                    file_id=file_id,
                    user_id=user_id
                )
                files[file_id] = (fh.filename, obj['name'])
        # Copy files if output directory is given.
        if output is not None:
            util.copy_files(files.values(), output)
        click.echo('\nOutput files\n------------')
        for filename, rel_path in files.values():
            if output is not None:
                filename = os.path.join(output, rel_path)
            click.echo(filename)
