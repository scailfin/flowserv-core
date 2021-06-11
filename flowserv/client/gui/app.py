# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Main routine to run the flowApp application from the command line. Use the
following command to run flowApp as a streamlit web application:

streamlit run flowapp/app.py [ -- [-a | --key=] <application-identifier>]

The application identifier references the workflow that is being run. This
workflow has to have been installed prior to running the app (e.g., using the
flowserv install CLI command). If you run the application without providing the
application identifier as a command-line argument the identifier is expected to
be in the environment variable FLOWSER_APP.
"""

from appdirs import user_cache_dir

import argparse
import os
import streamlit as st
import sys

from flowserv.client.app.base import Flowserv
from flowserv.client.app.workflow import Workflow
from flowserv.model.database import DB, SQLITE_DB
from flowserv.volume.manager import FStore, DEFAULT_STORE
from flowserv.client.gui.forms import show_form
from flowserv.client.gui.widget import display_runfiles

import flowserv.config as config
import flowserv.util as util


@st.cache(allow_output_mutation=True)
def get_app(source: str, specfile: str, manifestfile: str, name: str) -> Workflow:
    """Get the application handle for the given workflow template.

    Creates a fresh database in the application cache directory and installes
    the given workflow.

    Parameters
    ----------
    source: string
        Path to local template, name or URL of the template in the
        repository.
    specfile: string
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)
    manifestfile: string
        Path to manifest file. If not given an attempt is made to read one
        of the default manifest file names in the base directory.
    name: string
        Name of the application that will determine the base folder for all
        workflow files.

    Returns
    -------
    flowserv.client.app.workflow.Workflow
    """
    # Use the application cache directory for storing all workflow files.
    homedir = os.path.join(user_cache_dir(appname='flowapp'), name)
    util.cleardir(homedir)
    # Create new database.
    dburl = SQLITE_DB(dirname=homedir, filename='flowapp.db')
    DB(connect_url=dburl).init()
    # Install workflow template and return app handle.
    env = config\
        .env()\
        .basedir(homedir)\
        .database(dburl)\
        .volume(FStore(basedir=homedir, identifier=DEFAULT_STORE))\
        .open_access()\
        .run_sync()
    client = Flowserv(env=env)
    workflow_id = client.install(
        source=source,
        specfile=specfile,
        manifestfile=manifestfile
    )
    return client.open(identifier=workflow_id)


def main(source: str, specfile: str, manifestfile: str, name: str):
    """Run application that is identified by the given key."""
    st.set_option('deprecation.showfileUploaderEncoding', False)
    app = get_app(
        source=source,
        specfile=specfile,
        manifestfile=manifestfile,
        name=name
    )
    # Show application title, description, and instructions.
    st.title(app.name())
    st.header(app.description())
    st.markdown(app.instructions())
    # Render the main input form. The result is a boolean flag indicating if
    # the submit button was clicked and providing a mapping from template
    # parameter identifier to the submitted value in the respective input
    # form element.
    submit, arguments = show_form(app.parameters().sorted())
    if submit:
        # Run the workflow with the submitted parameter values. Show a spinner
        # while the workflow runs.
        with st.spinner('Running ...'):
            run = app.start_run(arguments)
        # Check if the workflow completed successfully or with errors.
        if run.is_error():
            # Display error messages.
            st.error('\n'.join(run.messages()))
        else:
            # Display run result files.
            st.header('Run Results')
            display_runfiles(run)
            postrun = app.get_postproc_results()
            if postrun:
                st.header('Post-Processing Results')
                display_runfiles(postrun)
        # Show a clear button that allows to remove displayed results from a
        # previous run.
        if st.button('clear', key='clear'):
            submit = False


if __name__ == '__main__':
    # Parse command line args to get the optional application key.
    args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--source", type=str, required=True)
    parser.add_argument("-s", "--specfile", type=str, default=None, required=False)
    parser.add_argument("-m", "--manifest", type=str, default=None, required=False)
    parser.add_argument("-n", "--name", type=str, default='app', required=False)
    parsed_args = parser.parse_args(args)
    # Run the main application.
    main(
        source=parsed_args.source,
        specfile=parsed_args.specfile,
        manifestfile=parsed_args.manifest,
        name=parsed_args.name
    )
