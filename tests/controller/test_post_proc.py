# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for post-processing workflows."""

import json
import os
import time

from flowserv.service.api import API
from flowserv.tests.files import FakeStream

import flowserv.config.api as config
import flowserv.model.workflow.state as st
import flowserv.tests.db as db


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
SPEC_FILE = os.path.join(DIR, '../.files/benchmark/postproc/benchmark.yaml')
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


# Default users
UID = '0000'

# List of names for input files
NAMES = ['Alice', 'Bob', 'Gabriel', 'William']


def test_postproc_workflow(tmpdir):
    """Execute the modified helloworld example."""
    # -- Setup ----------------------------------------------------------------
    # Create the database and service API with a serial workflow engine in
    # asynchronous mode
    os.environ[config.FLOWSERV_API_BASEDIR] = os.path.abspath(str(tmpdir))
    api = API(con=db.init_db(str(tmpdir), users=[UID]).connect())
    # Create workflow template
    wh = api.workflows().create_workflow(
        name='W1',
        sourcedir=TEMPLATE_DIR,
        specfile=SPEC_FILE
    )
    w_id = wh['id']
    # Create four groups and run the workflow with a slightly different input
    # file
    for i in range(4):
        name = 'G{}'.format(i)
        gh = api.groups().create_group(
            workflow_id=w_id,
            name=name,
            user_id=UID
        )
        g_id = gh['id']
        # Upload the names file
        fh = api.uploads().upload_file(
            group_id=g_id,
            file=FakeStream(data=NAMES[:(i+1)], format='txt/plain'),
            name='names.txt',
            user_id=UID
        )
        file_id = fh['id']
        # Set the template argument values
        arguments = [
            {'id': 'names', 'value': file_id},
            {'id': 'greeting', 'value': 'Hi'}
        ]
        # Run the workflow
        run = api.runs().start_run(
            group_id=g_id,
            arguments=arguments,
            user_id=UID
        )
        r_id = run['id']
        # Poll workflow state every second.
        while run['state'] in st.ACTIVE_STATES:
            time.sleep(1)
            run = api.runs().get_run(run_id=r_id, user_id=UID)
        assert run['state'] == st.STATE_SUCCESS
        wh = api.workflows().get_workflow(workflow_id=w_id)
        # print(json.dumps(wh, indent=4))
    del os.environ[config.FLOWSERV_API_BASEDIR]
