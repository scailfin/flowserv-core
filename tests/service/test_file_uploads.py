# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for uploaded files that are associated with workflow groups."""

import json
import os
import pytest

from flowserv.service.api import API
from flowserv.tests.controller import StateEngine
from flowserv.tests.files import FakeStream

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.tests.db as db
import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default users
USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()


def test_workflow_group_file_upload(tmpdir):
    """Test lift cycle for uploaded files."""
    # Initialize the API
    con = db.init_db(str(tmpdir), users=[USER_1, USER_2]).connect()
    engine = StateEngine()
    api = API(con=con, engine=engine)
    # Create two workflows with two groups each
    r = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    wf1 = r['id']
    r = api.workflows().create_workflow(name='W2', sourcedir=TEMPLATE_DIR)
    wf2 = r['id']
    # Create two groups for each workflow
    r = api.groups().create_group( workflow_id=wf1, name='G1', user_id=USER_1)
    w1g1 = r['id']
    r = api.groups().create_group( workflow_id=wf1, name='G2', user_id=USER_2)
    w1g2 = r['id']
    r = api.groups().create_group( workflow_id=wf2, name='G1', user_id=USER_1)
    w2g1 = r['id']
    r = api.groups().create_group( workflow_id=wf2, name='G2', user_id=USER_2)
    w2g2 = r['id']
    # Upload increasing number of files for each of the groups
    groups = [w1g1, w1g2, w2g1, w2g2]
    users = USER_1, USER_2, USER_1, USER_2
    files = list()
    for i in range(len(groups)):
        g_id = groups[i]
        u_id = users[i]
        for j in range(i+1):
            stream = FakeStream(data={'i': i, 'j': j})
            name = 'i{}-j{}.json'.format(i, j)
            r = api.uploads().upload_file(
                group_id=g_id,
                file=stream,
                name=name,
                user_id=u_id
            )
            serialize.validate_file_handle(r)
            assert r['name'] == name
            files.append((r['id'], g_id, u_id, stream.data))
    # Error when trying to upload file as no-member
    with pytest.raises(err.UnauthorizedAccessError):
        api.uploads().upload_file(
            group_id=w1g1,
            file=FakeStream(data={'a': 1}),
            name='f',
            user_id=USER_2
        )
    # Get file listings for individual groups
    for i in range(len(groups)):
        g_id = groups[i]
        u_id = users[i]
        r = api.uploads().list_files(group_id=g_id, user_id=u_id)
        serialize.validate_file_listing(r, count=i+1)
    # Error when trying to get listing as non-member
    with pytest.raises(err.UnauthorizedAccessError):
        api.uploads().list_files(group_id=w1g1, user_id=USER_2)
    # Check file content
    for f_id, g_id, u_id, data in files:
        fh, r = api.uploads().get_file(
            group_id=g_id,
            file_id=f_id,
            user_id=u_id
        )
        serialize.validate_file_handle(r)
        with open(fh.filename, 'r') as f:
            assert json.load(f) == data
    # Error trying to access file as non-member
    with pytest.raises(err.UnauthorizedAccessError):
         api.uploads().get_file(
            group_id=w1g1,
            file_id=files[0][0],
            user_id=USER_2
        )
    # Delete one file for eacg group
    deleted_files = list()
    j = 0
    for i in range(len(groups)):
        g_id = groups[i]
        u_id = users[i]
        f_id = files[j][0]
        api.uploads().delete_file(
            group_id=g_id,
            file_id=f_id,
            user_id=u_id
        )
        deleted_files.append((f_id, g_id, u_id))
        j += (i + 1)
    for i in range(len(groups)):
        g_id = groups[i]
        u_id = users[i]
        r = api.uploads().list_files(group_id=g_id, user_id=u_id)
        serialize.validate_file_listing(r, count=i)
    # Errors when accessing or deleting unknown files
    for f_id, g_id, u_id in deleted_files:
        with pytest.raises(err.UnknownFileError):
            api.uploads().get_file(group_id=g_id, file_id=f_id, user_id=u_id)
        with pytest.raises(err.UnknownFileError):
            api.uploads().delete_file(
                group_id=g_id,
                file_id=f_id,
                user_id=u_id
            )
