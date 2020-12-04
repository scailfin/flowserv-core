# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the local workflow service API."""

import flowserv.tests.serialize as serialize


def test_get_workflow_view(local_service, hello_world):
    """Test serialization for created workflows."""
    # -- Create workflow with minimal metadata --------------------------------
    with local_service() as api:
        wf = hello_world(api, name='W1')
        r = api.workflows().get_workflow(wf.workflow_id)
        serialize.validate_workflow_handle(doc=r, has_optional=False)
        assert len(r['parameterGroups']) == 1
        serialize.validate_para_module(r['parameterGroups'][0])
        assert len(r['parameters']) == 3
        for para in r['parameters']:
            serialize.validate_parameter(para)


def test_list_workflows_view(local_service, hello_world):
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
