# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the local workflow service API."""

import flowserv.tests.serialize as serialize


def test_delete_workflow_local(local_service, hello_world):
    """Test deleting a workflow from the repository."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two instances of the 'Hello World' workflow.
    with local_service() as api:
        workflow = hello_world(api, name='W1')
        workflow_id = workflow.workflow_id
        hello_world(api, name='W2')
    # -- Delete the first workflow --------------------------------------------
    with local_service() as api:
        api.workflows().delete_workflow(workflow_id)
        # After deletion one workflow is left.
        r = api.workflows().list_workflows()
        assert len(r['workflows']) == 1


def test_get_workflow_local(local_service, hello_world):
    """Test serialization for created workflows."""
    # -- Create workflow with minimal metadata --------------------------------
    with local_service() as api:
        workflow = hello_world(api, name='W1')
        workflow_id = workflow.workflow_id
        r = api.workflows().get_workflow(workflow_id)
        serialize.validate_workflow_handle(doc=r, has_optional=False)
        assert len(r['parameterGroups']) == 1
        serialize.validate_para_module(r['parameterGroups'][0])
        assert len(r['parameters']) == 3
        for para in r['parameters']:
            serialize.validate_parameter(para)


def test_list_workflows_local(local_service, hello_world):
    """Test serialization for workflow listings."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two instances of the 'Hello World' workflow.
    with local_service() as api:
        hello_world(api, name='W1')
        hello_world(api, name='W2')
    # -- Workflow Listing -----------------------------------------------------
    with local_service() as api:
        r = api.workflows().list_workflows()
        serialize.validate_workflow_listing(doc=r)
        assert len(r['workflows']) == 2


def test_update_workflow_local(local_service, hello_world):
    """Test updating workflow properties."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one instances of the 'Hello World' workflow with minimal metadata.
    with local_service() as api:
        workflow = hello_world(api, name='W1')
        workflow_id = workflow.workflow_id
        r = api.workflows().get_workflow(workflow_id)
        assert 'description' not in r
        assert 'instructions' not in r
    # -- Update workflow ------------------------------------------------------
    with local_service() as api:
        r = api.workflows().update_workflow(
            workflow_id=workflow_id,
            name='Hello World',
            description='Simple Hello World Demo',
            instructions='Just run it'
        )
        assert r['name'] == 'Hello World'
        assert r['description'] == 'Simple Hello World Demo'
        assert r['instructions'] == 'Just run it'
