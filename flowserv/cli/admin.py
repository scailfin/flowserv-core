# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface for the administrative tasks to configure the
environment, intialize the underlying database, and to create and maintain
workflows in the repository.
"""

import click
import logging
import os
import sys
import tempfile

from flowserv.cli.app import appcli
from flowserv.cli.config import get_configuration
from flowserv.cli.parameter import read
from flowserv.cli.repository import list_repository
from flowserv.cli.run import runscli
from flowserv.cli.user import register_user
from flowserv.cli.workflow import add_workflow, workflowcli
from flowserv.config.api import API_BASEDIR, FLOWSERV_API_BASEDIR
from flowserv.config.controller import FLOWSERV_ASYNC
from flowserv.config.database import FLOWSERV_DB
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
def configuration():
    """Print configuration variables for flowServ."""
    comment = '\n#\n# {}\n#\n'
    envvar = 'export {}={}'
    for title, envs in get_configuration().items():
        click.echo(comment.format(title))
        for var, val in envs.items():
            click.echo(envvar.format(var, val))
    click.echo()


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
    os.makedirs(API_BASEDIR(), exist_ok=True)


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
def run_workflow(specfile, manifest, ignorepp, output, source):
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
            name=util.get_unique_identifier(),
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


# App
cli.add_command(appcli)


# Repository
cli.add_command(list_repository, name='repository')


# Runs
cli.add_command(runscli)


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
                fh, fileobj = api.runs().get_result_file(
                    run_id=run_id,
                    file_id=file_id,
                    user_id=user_id
                )
                files[file_id] = (fileobj, obj['name'])
        # Copy files if output directory is given.
        if output is not None:
            util.copy_files(files.values(), output)
        click.echo('\nOutput files\n------------')
        for filename, rel_path in files.values():
            if output is not None:
                filename = os.path.join(output, rel_path)
            click.echo(filename)
