# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Main module for the flowserv command line interface. Defines the top-level
command group and the API context for all commands.
"""

from typing import Dict

import click
import os

from flowserv.client.cli.admin import configuration, init
from flowserv.client.cli.app import cli_app
from flowserv.client.cli.cleanup import cli_cleanup
from flowserv.client.cli.group import cli_group
from flowserv.client.cli.gui import run_template
from flowserv.client.cli.repository import list_repository
from flowserv.client.cli.run import cli_run
from flowserv.client.cli.uploads import cli_uploads
from flowserv.client.cli.user import cli_user, login_user, logout_user, whoami_user
from flowserv.client.cli.workflow import cli_benchmark, cli_workflow

import flowserv.config as config
import flowserv.error as err


# -- CLI environment context --------------------------------------------------

class EnvContext(object):
    """Helper class to get default parameter values for from the environment.
    The different applications ``flowserv`` and ``rob`` historically use
    different environment variables to maintain the workflow and group identifier.
    This class is used to hide these details from the CLI commands.
    """
    def __init__(self, vars: Dict):
        """Initialize the mapping of objects ('workflow' and 'group') to the
        respective environment variables that hold the object identifier.

        Parameters
        ----------
        vars: dict
            Mapping of object types to environment variable names.
        """
        self.vars = vars
        # Set the access token variable.
        self.vars['token'] = config.FLOWSERV_ACCESS_TOKEN

    def access_token(self) -> str:
        """Get the value for the user access token from the environment.

        The environment variable that holds the token (*FLOWSERV_ACCESS_TOKEN*)
        is the same accross both applications.

         Raises a missing configuration error if the value is not set.

        Returns
        -------
        string
        """
        access_token = os.environ.get(self.vars['token'])
        if not access_token:
            raise err.MissingConfigurationError('access token')
        return access_token

    def get_group(self, params: Dict) -> str:
        """Get the user group (submission) identifier from the parameters or
        the environment.

        Attempts to get the group identifier from the list of parsed parameters.
        Assumes the the identifier is given by the parameter ``group``. If no
        group identifier is present in the given parameter dictionary an
        attempt is made to get the identifier from the environment variable that
        is defined for user groups in this context. If that environment variable
        is not set an attempt is made to get the value from the environment
        variable for the workflow identifier. The latter is done since flowserv
        applications are installed with identical workflow and group identifier.

        Raises a missing configuration error if the value is not set.

        Parameters
        ----------
        params: dict
            Dictionary of parsed command-line parameters

        Returns
        -------
        string
        """
        # Get the parameter value for the group identifier. The value will
        # be set to None if the parameter is defined for a command but was not
        # provided by the user.
        group_id = params.get('group')
        # If the group value is None, attempt to get it from the environment.
        if not group_id:
            group_id = os.environ.get(self.vars['group'])
        # If the group value is still None, attempt to get it from the
        # workflow environment variable.
        if not group_id:
            group_id = os.environ.get(self.vars['workflow'])
        # Raise an error if no group identifier was found.
        if not group_id:
            raise err.MissingConfigurationError('submission (group) identifier')
        return group_id

    def get_workflow(self, params: Dict) -> str:
        """Get the workflow identifier from the parameters or the environment.

        Attempts to get the workflow identifier from the list of parsed parameters.
        Assumes the the identifier is given by the parameter ``workflow``. If no
        workflow identifier is present in the given parameter dictionary an
        attempt is made to get the identifier from the environment variable that
        is defined for workflows in this context.

        Raises a missing configuration error if the value is not set.

        Parameters
        ----------
        params: dict
            Dictionary of parsed command-line parameters

        Returns
        -------
        string
        """
        # Get the parameter value for the workflow identifier. The value will
        # be set to None if the parameter is defined for a command but was not
        # provided by the user.
        workflow_id = params.get('workflow')
        # If the workflow value is None, attempt to get it from the environment.
        if not workflow_id:
            workflow_id = os.environ.get(self.vars['workflow'])
        # Raise an error if no workflow identifier was found.
        if not workflow_id:
            raise err.MissingConfigurationError('workflow identifier')
        return workflow_id


# -- flowserv -----------------------------------------------------------------

@click.group()
@click.pass_context
def cli_flowserv(ctx):
    """Command line interface for flowServ."""
    ctx.obj = EnvContext(
        vars={'workflow': config.FLOWSERV_APP, 'group': config.FLOWSERV_GROUP}
    )


# Administrative tasks (init, config, and cleanup)
cli_flowserv.add_command(configuration, name='config')
cli_flowserv.add_command(init, name='init')
cli_flowserv.add_command(cli_cleanup, name='cleanup')

# Applications
cli_flowserv.add_command(cli_app, name='app')

# Users
cli_flowserv.add_command(login_user, name='login')
cli_flowserv.add_command(logout_user, name='logout')
cli_flowserv.add_command(cli_user, name='users')
cli_flowserv.add_command(whoami_user, name='whoami')

# User groups
cli_flowserv.add_command(cli_group, 'groups')

# Group files
cli_flowserv.add_command(cli_uploads, 'files')

# Graphical User Interface
cli_flowserv.add_command(run_template, name='gui')

# Workflow repository listing
cli_flowserv.add_command(list_repository, name='repo')

# Workflows
cli_flowserv.add_command(cli_workflow, name='workflows')

# Workflow Runs
cli_flowserv.add_command(cli_run, name='runs')


# -- rob ----------------------------------------------------------------------

@click.group()
@click.pass_context
def cli_rob(ctx):
    """Command line interface for ROB."""
    ctx.obj = EnvContext(
        vars={'workflow': config.ROB_BENCHMARK, 'group': config.ROB_SUBMISSION}
    )


# Benchmarks
cli_rob.add_command(cli_benchmark, name='benchmarks')

# Benchmark Runs
cli_rob.add_command(cli_run, name='runs')

# Users
cli_rob.add_command(login_user, name='login')
cli_rob.add_command(logout_user, name='logout')
cli_rob.add_command(cli_user, name='users')
cli_rob.add_command(whoami_user, name='whoami')

# Submissions
cli_rob.add_command(cli_group, 'submissions')

# Group files
cli_rob.add_command(cli_uploads, 'files')
