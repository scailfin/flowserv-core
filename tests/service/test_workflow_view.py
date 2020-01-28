# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow service API."""

import os

from flowserv.service.api import API
from flowserv.tests.controller import StateEngine

import flowserv.tests.db as db

import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_workflow_view(tmpdir):
    """Test serialization for created workflows and workflow listings."""
    # Get an API instance that uses the StateEngine as the backend
    con = db.init_db(str(tmpdir)).connect()
    engine = StateEngine()
    api = API(con=con, engine=engine, basedir=str(tmpdir))
    # Create two copies of the same workflow
    r = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    serialize.validate_workflow_handle(doc=r, has_optional=False)
    r = api.workflows().get_workflow(r['id'])
    serialize.validate_workflow_handle(doc=r, has_optional=False)
    assert len(r['modules']) == 1
    serialize.validate_para_module(r['modules'][0])
    assert len(r['parameters']) == 3
    for para in r['parameters']:
        serialize.validate_parameter(para)
    r = api.workflows().create_workflow(
        name='W2',
        description='ABC',
        instructions='XYZ',
        sourcedir=TEMPLATE_DIR
    )
    serialize.validate_workflow_handle(doc=r, has_optional=True)
    assert r['description'] == 'ABC'
    assert r['instructions'] == 'XYZ'
    workflow_id = r['id']
    # Workflow Listing
    r = api.workflows().list_workflows()
    serialize.validate_workflow_listing(doc=r)
    assert len(r['workflows']) == 2
    # Delete workflow
    api.workflows().delete_workflow(workflow_id)
    r = api.workflows().list_workflows()
    assert len(r['workflows']) == 1
