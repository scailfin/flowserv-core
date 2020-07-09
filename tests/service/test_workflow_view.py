# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow service API."""

import os

import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
INSTRUCTION_FILE = os.path.join(DIR, '../.files/benchmark/instructions.txt')


def test_workflow_view(api_factory, hello_world):
    """Test serialization for created workflows and workflow listings."""
    # Get an API instance that uses the StateEngine as the backend
    api = api_factory()
    # Create two copies of the same workflow
    r = hello_world(api, name='W1')
    serialize.validate_workflow_handle(doc=r, has_optional=False)
    r = api.workflows().get_workflow(r['id'])
    serialize.validate_workflow_handle(doc=r, has_optional=False)
    assert len(r['modules']) == 1
    serialize.validate_para_module(r['modules'][0])
    assert len(r['parameters']) == 3
    for para in r['parameters']:
        serialize.validate_parameter(para)
    r = hello_world(
        api=api,
        name='W2',
        description='ABC',
        instructions=INSTRUCTION_FILE
    )
    serialize.validate_workflow_handle(doc=r, has_optional=True)
    assert r['description'] == 'ABC'
    assert r['instructions'] == 'How to run Hello World'
    workflow_id = r['id']
    # -- Update workflow ------------------------------------------------------
    r = api.workflows().update_workflow(
        workflow_id=workflow_id,
        name='Hello World',
        description='Simple Hello World Demo',
        instructions='Just run it'
    )
    assert r['name'] == 'Hello World'
    assert r['description'] == 'Simple Hello World Demo'
    assert r['instructions'] == 'Just run it'
    # -- Workflow Listing -----------------------------------------------------
    r = api.workflows().list_workflows()
    serialize.validate_workflow_listing(doc=r)
    assert len(r['workflows']) == 2
    # -- Delete workflow ------------------------------------------------------
    api.workflows().delete_workflow(workflow_id)
    r = api.workflows().list_workflows()
    assert len(r['workflows']) == 1
