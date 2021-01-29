# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for workflow evaluation rankings."""

from flowserv.model.template.schema import SortColumn
from flowserv.tests.service import create_ranking, create_user

import flowserv.tests.serialize as serialize


def test_workflow_result_ranking(local_service, hello_world):
    """Test creating rankings from multiple workflow runs."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create four groups for the 'Hello World' workflow with one successful
    # run each.
    with local_service() as api:
        user_1 = create_user(api)
        workflow_id = hello_world(api).workflow_id
    with local_service(user_id=user_1) as api:
        groups = create_ranking(api, workflow_id, 4)
    # -- Get ranking in decreasing order of avg_count -------------------------
    with local_service() as api:
        r = api.workflows().get_ranking(
            workflow_id=workflow_id,
            order_by=[SortColumn('avg_count')],
            include_all=False
        )
        serialize.validate_ranking(r)
        ranking = [e['group']['id'] for e in r['ranking']]
        assert groups == ranking[::-1]
    # -- Get ranking in decreasing order of max_len ---------------------------
    with local_service() as api:
        r = api.workflows().get_ranking(
            workflow_id=workflow_id,
            order_by=[SortColumn('max_len')],
            include_all=False
        )
        serialize.validate_ranking(r)
        ranking = [e['group']['id'] for e in r['ranking']]
        assert groups == ranking
