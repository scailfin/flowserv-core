# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for post-processing workflows."""

import os
import pytest
import time

from flowserv.model.database import TEST_DB
from flowserv.service.local import service
from flowserv.service.run.argument import serialize_arg, serialize_fh
from flowserv.tests.files import io_file
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run, upload_file
)

import flowserv.config as config
import flowserv.util as util
import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
SPEC_FILE = os.path.join(DIR, '../.files/benchmark/postproc/benchmark.yaml')
SPEC_FILE_ERR_1 = os.path.join(DIR, '../.files/benchmark/postproc/error1.yaml')
SPEC_FILE_ERR_2 = os.path.join(DIR, '../.files/benchmark/postproc/error2.yaml')
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


# List of names for input files
NAMES = ['Alice', 'Bob', 'Gabriel', 'William']


@pytest.mark.parametrize(
    'fsconfig',
    [{
        config.FLOWSERV_FILESTORE_MODULE: 'flowserv.model.files.fs',
        config.FLOWSERV_FILESTORE_CLASS: 'FileSystemStore'
    }, {
        config.FLOWSERV_FILESTORE_MODULE: 'flowserv.model.files.s3',
        config.FLOWSERV_FILESTORE_CLASS: 'BucketStore'
    }]
)
def test_postproc_workflow(fsconfig, tmpdir):
    """Execute the modified helloworld example."""
    # -- Setup ----------------------------------------------------------------
    #
    os.environ[config.FLOWSERV_API_BASEDIR] = str(tmpdir)
    os.environ[config.FLOWSERV_DB] = TEST_DB(tmpdir)
    os.environ[config.FLOWSERV_FILESTORE_MODULE] = fsconfig[config.FLOWSERV_FILESTORE_MODULE]
    os.environ[config.FLOWSERV_FILESTORE_CLASS] = fsconfig[config.FLOWSERV_FILESTORE_CLASS]
    os.environ[config.FLOWSERV_ASYNC] = 'True'
    from flowserv.service.database import init_db
    init_db().init()
    from flowserv.service.backend import init_backend
    init_backend()
    # Start a new run for the workflow template.
    with service() as api:
        # Need to set the file store in the backend to the new instance as
        # well. Otherwise, the post processing workflow may attempt to use
        # the backend which was initialized prior with a different file store.
        workflow_id = create_workflow(
            api,
            source=TEMPLATE_DIR,
            specfile=SPEC_FILE
        )
        user_id = create_user(api)
    # Create four groups and run the workflow with a slightly different input
    # file
    for i in range(4):
        with service(user_id=user_id) as api:
            group_id = create_group(api, workflow_id)
            names = io_file(data=NAMES[:(i + 1)], format='plain/text')
            file_id = upload_file(api, group_id, names)
            # Set the template argument values
            arguments = [
                serialize_arg('names', serialize_fh(file_id)),
                serialize_arg('greeting', 'Hi')
            ]
            run_id = start_run(api, group_id, arguments=arguments)
        # Poll workflow state every second.
        run = poll_run(run_id, user_id)
        assert run['state'] == st.STATE_SUCCESS
        with service() as api:
            wh = api.workflows().get_workflow(workflow_id=workflow_id)
        attmpts = 0
        while 'postproc' not in wh:
            time.sleep(1)
            with service() as api:
                wh = api.workflows().get_workflow(workflow_id=workflow_id)
            attmpts += 1
            if attmpts > 60:
                break
        assert 'postproc' in wh
        serialize.validate_workflow_handle(wh)
        attmpts = 0
        while wh['postproc']['state'] in st.ACTIVE_STATES:
            time.sleep(1)
            with service() as api:
                wh = api.workflows().get_workflow(workflow_id=workflow_id)
            attmpts += 1
            if attmpts > 60:
                break
        serialize.validate_workflow_handle(wh)
        for fobj in wh['postproc']['files']:
            if fobj['name'] == 'results/compare.json':
                file_id = fobj['id']
        with service(user_id=user_id) as api:
            fh = api.runs().get_result_file(
                run_id=wh['postproc']['id'],
                file_id=file_id
            )
        compare = util.read_object(fh)
        assert len(compare) == (i + 1)
    # -- Clean up
    del os.environ[config.FLOWSERV_API_BASEDIR]
    del os.environ[config.FLOWSERV_DB]
    del os.environ[config.FLOWSERV_FILESTORE_MODULE]
    del os.environ[config.FLOWSERV_FILESTORE_CLASS]
    del os.environ[config.FLOWSERV_ASYNC]


def test_postproc_workflow_errors(tmpdir):
    """Execute the modified helloworld example."""
    # -- Setup ----------------------------------------------------------------
    #
    os.environ[config.FLOWSERV_API_BASEDIR] = str(tmpdir)
    os.environ[config.FLOWSERV_DB] = TEST_DB(tmpdir)
    os.environ[config.FLOWSERV_ASYNC] = 'True'
    from flowserv.service.database import init_db
    init_db().init()
    from flowserv.service.backend import init_backend
    init_backend()
    # Start a new run for the workflow template.
    # Error during data preparation
    run_erroneous_workflow(SPEC_FILE_ERR_1)
    # Erroneous specification
    run_erroneous_workflow(SPEC_FILE_ERR_2)
    # -- Clean up
    del os.environ[config.FLOWSERV_API_BASEDIR]
    del os.environ[config.FLOWSERV_DB]
    del os.environ[config.FLOWSERV_ASYNC]


# -- Helper functions ---------------------------------------------------------

def poll_run(run_id, user_id):
    """Poll workflow run while in active state."""
    with service(user_id=user_id) as api:
        run = api.runs().get_run(run_id=run_id)
    while run['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        with service(user_id=user_id) as api:
            run = api.runs().get_run(run_id=run_id)
    return run


def run_erroneous_workflow(specfile):
    """Execute the modified helloworld example."""
    with service() as api:
        # Create workflow template, user, and the workflow group.
        workflow_id = create_workflow(
            api,
            source=TEMPLATE_DIR,
            specfile=specfile
        )
        user_id = create_user(api)
    with service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        # Upload the names file.
        names = io_file(data=NAMES, format='txt/plain')
        file_id = upload_file(api, group_id, names)
        # Run the workflow.
        arguments = [
            serialize_arg('names', serialize_fh(file_id)),
            serialize_arg('greeting', 'Hi')
        ]
        run_id = start_run(api, group_id, arguments=arguments)
    # Poll workflow state every second.
    run = poll_run(run_id, user_id)
    assert run['state'] == st.STATE_SUCCESS
    with service() as api:
        wh = api.workflows().get_workflow(workflow_id=workflow_id)
    attmpts = 0
    while 'postproc' not in wh:
        time.sleep(1)
        with service() as api:
            wh = api.workflows().get_workflow(workflow_id=workflow_id)
        attmpts += 1
        if attmpts > 60:
            break
    assert 'postproc' in wh
    serialize.validate_workflow_handle(wh)
    attmpts = 0
    while wh['postproc']['state'] in st.ACTIVE_STATES:
        time.sleep(1)
        with service() as api:
            wh = api.workflows().get_workflow(workflow_id=workflow_id)
        attmpts += 1
        if attmpts > 60:
            break
    assert wh['postproc']['state'] not in st.ACTIVE_STATES
    serialize.validate_workflow_handle(wh)
    assert wh['postproc']['state'] == st.STATE_ERROR
