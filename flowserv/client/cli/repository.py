# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to list contents of the global workflow template
repository.
"""

import click

from flowserv.model.workflow.repository import WorkflowRepository


# -- List elements in the global repository -----------------------------------

@click.command()
def list_repository():
    """List template repository contents."""
    for identifier, description, _ in WorkflowRepository().list():
        click.echo('{}\t{}'.format(identifier, description))
