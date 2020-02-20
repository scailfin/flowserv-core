# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for post-processing workflows."""

import os
import time

from flowserv.service.api import API
from flowserv.tests.files import FakeStream

import flowserv.config.api as config
import flowserv.core.util as util
import flowserv.model.workflow.state as st
import flowserv.tests.db as db
import flowserv.tests.serialize as serialize


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
SPEC_FILE = os.path.join(DIR, '../.files/benchmark/postproc/benchmark.yaml')
SPEC_FILE_ERR_1 = os.path.join(DIR, '../.files/benchmark/postproc/error1.yaml')
SPEC_FILE_ERR_2 = os.path.join(DIR, '../.files/benchmark/postproc/error2.yaml')
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
        while 'postproc' not in wh:
            time.sleep(1)
            wh = api.workflows().get_workflow(workflow_id=w_id)
        serialize.validate_workflow_handle(wh)
        while wh['postproc']['state'] in st.ACTIVE_STATES:
            time.sleep(1)
            wh = api.workflows().get_workflow(workflow_id=w_id)
        serialize.validate_workflow_handle(wh)
        for res in wh['postproc']['resources']:
            if res['name'] == 'results/compare.json':
                res_id = res['id']
        fh = api.runs().get_result_file(
            run_id=wh['postproc']['id'],
            resource_id=res_id,
            user_id=None
        )
        compare = util.read_object(fh.filename)
        assert len(compare) == (i + 1)
    del os.environ[config.FLOWSERV_API_BASEDIR]


def test_postproc_workflow_errors(tmpdir):
    """Execute the modified helloworld example."""
    # Create the database and service API with a serial workflow engine in
    # asynchronous mode
    os.environ[config.FLOWSERV_API_BASEDIR] = os.path.abspath(str(tmpdir))
    api = API(con=db.init_db(str(tmpdir), users=[UID]).connect())
    # Error during data preparation
    run_erroneous_workflow(api, SPEC_FILE_ERR_1)
    # Erroneous specification
    run_erroneous_workflow(api, SPEC_FILE_ERR_2)
    del os.environ[config.FLOWSERV_API_BASEDIR]


# -- Helper functions ---------------------------------------------------------

def run_erroneous_workflow(api, specfile):
    """Execute the modified helloworld example."""
    # Create workflow template
    wh = api.workflows().create_workflow(
        name=util.get_unique_identifier(),
        sourcedir=TEMPLATE_DIR,
        specfile=specfile
    )
    w_id = wh['id']
    # Create one group and run the workflow
    gh = api.groups().create_group(workflow_id=w_id, name='G', user_id=UID)
    g_id = gh['id']
    # Upload the names file
    fh = api.uploads().upload_file(
        group_id=g_id,
        file=FakeStream(data=NAMES, format='txt/plain'),
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
        print('active run')
        time.sleep(1)
        run = api.runs().get_run(run_id=r_id, user_id=UID)
    assert run['state'] == st.STATE_SUCCESS
    wh = api.workflows().get_workflow(workflow_id=w_id)
    while 'postproc' not in wh:
        print('wait for postproc run to start')
        time.sleep(1)
        wh = api.workflows().get_workflow(workflow_id=w_id)
    serialize.validate_workflow_handle(wh)
    while wh['postproc']['state'] in st.ACTIVE_STATES:
        print('postproc run active')
        time.sleep(1)
        wh = api.workflows().get_workflow(workflow_id=w_id)
    serialize.validate_workflow_handle(wh)
    assert wh['postproc']['state'] == st.STATE_ERROR
