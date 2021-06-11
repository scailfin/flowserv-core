# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Command line interface to run a workflow template using a web GUI based on
streamlit.
"""

import click
import os


# -- Run template using GUI ---------------------------------------------------

@click.command()
@click.option(
    '-a', '--app_name',
    required=False,
    help='Name of the application.'
)
@click.option(
    '-s', '--specfile',
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
@click.argument('template')
def run_template(app_name, specfile, manifest, template):
    """Run workflow template in GUI."""
    import streamlit.cli
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, '..', 'gui', 'app.py')
    args = [f'--sourc={template}']
    if app_name:
        args.append(f'--name={app_name}')
    if specfile:
        args.append(f'--specfile={specfile}')
    if manifest:
        args.append(f'--manifest={manifest}')
    streamlit.cli._main_run(filename, args)
