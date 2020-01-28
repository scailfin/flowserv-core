# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow group service API."""

import os
import pytest

from flowserv.service.api import API
from flowserv.tests.controller import StateEngine
from flowserv.tests.parameter import StringParameter

import flowserv.tests.db as db

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.tests.serialize as serialize
import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default users
USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()


"""Define expected stes of labels in resource serializations."""
GROUP_DESCRIPTOR = [
    labels.ID,
    labels.NAME,
    labels.WORKFLOW,
    labels.LINKS
]
GROUP_HANDLE = GROUP_DESCRIPTOR + [labels.MEMBERS, labels.PARAMETERS, labels.FILES]
GROUP_RELS = [
    hateoas.SELF,
    hateoas.WORKFLOW,
    hateoas.action(hateoas.UPLOAD),
    hateoas.action(hateoas.SUBMIT)
]


def test_group_view(tmpdir):
    """Test serialization for created workflows groups and group listings."""
    # Get an API instance that uses the StateEngine as the backend
    con = db.init_db(str(tmpdir), users=[USER_1, USER_2]).connect()
    engine = StateEngine()
    api = API(con=con, engine=engine)
    # Create a new workflow group
    r = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    workflow_id = r[labels.ID]
    # Create a new group for the workflow
    r = api.groups().create_group(
        workflow_id=workflow_id,
        name='G1',
        user_id=USER_1
    )
    util.validate_doc(doc=r, mandatory=GROUP_HANDLE)
    serialize.validate_links(doc=r, keys=GROUP_RELS)
    assert len(r[labels.PARAMETERS]) == 3
    assert len(r[labels.MEMBERS]) == 1
    # Retrieve the workflow group handle from the service
    r = api.groups().get_group(r[labels.ID])
    util.validate_doc(doc=r, mandatory=GROUP_HANDLE)
    serialize.validate_links(doc=r, keys=GROUP_RELS)
    assert len(r[labels.PARAMETERS]) == 3
    assert len(r[labels.MEMBERS]) == 1
    g1 = r[labels.ID]
    # Create second group
    # Create second group with two members
    r = api.groups().create_group(
        workflow_id=workflow_id,
        name='G2',
        user_id=USER_1,
        members=[USER_2],
        parameters={
            'A': StringParameter('A'),
            'B': StringParameter('B')
        }
    )
    util.validate_doc(doc=r, mandatory=GROUP_HANDLE)
    serialize.validate_links(doc=r, keys=GROUP_RELS)
    assert len(r[labels.PARAMETERS]) == 5
    assert len(r[labels.MEMBERS]) == 2
    r = api.groups().get_group(r[labels.ID])
    util.validate_doc(doc=r, mandatory=GROUP_HANDLE)
    serialize.validate_links(doc=r, keys=GROUP_RELS)
    assert len(r[labels.PARAMETERS]) == 5
    assert len(r[labels.MEMBERS]) == 2
    # Get group listing listing for workflow
    r = api.groups().list_groups(workflow_id=workflow_id)
    util.validate_doc(doc=r, mandatory=[labels.GROUPS, labels.LINKS])
    assert len(r[labels.GROUPS]) == 2
    for g in r[labels.GROUPS]:
        util.validate_doc(doc=g, mandatory=GROUP_DESCRIPTOR)
        serialize.validate_links(doc=g, keys=GROUP_RELS)
    # Get groups for user 1 and 2
    r = api.groups().list_groups(user_id=USER_1)
    assert len(r[labels.GROUPS]) == 2
    r = api.groups().list_groups(user_id=USER_2)
    assert len(r[labels.GROUPS]) == 1
    # Update group name for G1 by user 2 will fail at first but succeed once
    # the user is a member of the group
    with pytest.raises(err.UnauthorizedAccessError):
        api.groups().update_group(group_id=g1, user_id=USER_2, name='ABC')
    api.groups().update_group(
        group_id=g1,
        user_id=USER_1,
        members=[USER_1, USER_2]
    )
    r = api.groups().update_group(group_id=g1, user_id=USER_2, name='ABC')
    assert r[labels.NAME] == 'ABC'
    r = api.groups().get_group(g1)
    assert r[labels.NAME] == 'ABC'
    assert len(r[labels.MEMBERS]) == 2
    # Delete the first group
    api.groups().delete_group(group_id=g1, user_id=USER_1)
    r = api.groups().list_groups(workflow_id=workflow_id)
    assert len(r[labels.GROUPS]) == 1
