# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Main module for the flowserv command line interface. Defines the top-level
command group and the API context for all commands.
"""

import click

from flowserv.client.cli.admin import configuration, init
from flowserv.client.cli.repository import list_repository


@click.group()
def cli():
    """Command line interface for flowServ."""
    pass


# -- Administrative tasks (init and config) -----------------------------------
cli.add_command(configuration, name='config')
cli.add_command(init, name='init')


# -- Workflow repository listing ----------------------------------------------
cli.add_command(list_repository, name='repo')
