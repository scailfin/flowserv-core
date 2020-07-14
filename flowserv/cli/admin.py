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
from flowserv.config.auth import FLOWSERV_AUTH_LOGINTTL, AUTH_LOGINTTL
from flowserv.config.backend import (
    FLOWSERV_BACKEND_CLASS, FLOWSERV_BACKEND_MODULE)
from flowserv.cli.workflow import workflowcli
from flowserv.model.database import DB, TEST_URL
from flowserv.model.parameter.base import create_parameter_index
from flowserv.service.api import service
from flowserv.service.run import ARG_AS, ARG_ID, ARG_VALUE

import flowserv.config as config
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
        conf.append((config.FLOWSERV_API_BASEDIR, config.API_BASEDIR()))
        api_name = config.API_NAME()
        conf.append((config.FLOWSERV_API_NAME, '"{}"'.format(api_name)))
        conf.append((config.FLOWSERV_API_HOST, config.API_HOST()))
        conf.append((config.FLOWSERV_API_PORT, config.API_PORT()))
        conf.append((config.FLOWSERV_API_PROTOCOL, config.API_PROTOCOL()))
        conf.append((config.FLOWSERV_API_PATH, config.API_PATH()))
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
            connect_url = config.DB_CONNECT()
        except err.MissingConfigurationError:
            connect_url = 'None'
        click.echo(envvar.format(config.FLOWSERV_DB, connect_url))
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
    '-d', '--dir',
    type=click.Path(exists=True, dir_okay=True, readable=True),
    help='Base directory for API files (overrides FLOWSERV_API_DIR).'
)
@click.option(
    '-f', '--force',
    is_flag=True,
    default=False,
    help='Create database without confirmation'
)
def init(dir=None, force=False):
    """Initialize database and base directories for the flowServ API. The
    configuration parameters for the database are taken from the respective
    environment variables. Creates the API base directory if it does not exist.
    """
    if not force:
        click.echo('This will erase an existing database.')
        click.confirm('Continue?', default=True, abort=True)
    # Create a new instance of the database
    try:
        print('init database')
        DB().init()
    except err.MissingConfigurationError as ex:
        click.echo(str(ex))
        sys.exit(-1)
    # If the base directory is given ensure that the directory exists
    if dir is not None:
        base_dir = dir
    else:
        base_dir = config.API_BASEDIR()
    util.create_dir(base_dir)


# -- Run workflow template for testing purposes -------------------------------

@cli.command(name='run')
@click.option(
    '-s', '--src',
    type=click.Path(exists=True, file_okay=False, readable=True),
    required=True,
    help='Workflow template directory.'
)
@click.option(
    '-f', '--specfile',
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=False,
    help='Optional path to workflow specification file.'
)
@click.option(
    '-o', '--output',
    type=click.Path(exists=False, file_okay=False, readable=True),
    required=False,
    help='Directory for output files.'
)
def run_workflow(src, specfile, output):
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
    os.environ[config.FLOWSERV_API_BASEDIR] = tmpdir
    os.environ[config.FLOWSERV_ASYNC] = 'False'
    os.environ[config.FLOWSERV_DB] = TEST_URL
    from flowserv.service.database import database
    database.init()
    # -- Add workflow template to repository ----------------------------------
    with service() as api:
        workflow = api.workflows().create_workflow(
            name=util.get_short_identifier(),
            sourcedir=src,
            specfile=specfile
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
    params = create_parameter_index(workflow['parameters'])
    params = sorted(params.values(), key=lambda p: (p.index, p.identifier))
    click.echo('\nWorkflow inputs\n---------------')
    args = read(params)
    # -- Upload files ---------------------------------------------------------
    runargs = list()
    for para in params:
        arg = {ARG_ID: para.identifier}
        if para.is_file():
            filename, target_path = args[para.identifier]
            with service() as api:
                file_id = api.uploads().upload_file(
                    group_id=submission_id,
                    file=filename,
                    name=os.path.basename(filename),
                    user_id=user_id
                )['id']
                arg[ARG_VALUE] = file_id
                if target_path is not None:
                    arg[ARG_AS] = target_path
        else:
            arg[ARG_VALUE] = args[para.identifier]
        runargs.append(arg)
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


# Workflows
cli.add_command(workflowcli)


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
