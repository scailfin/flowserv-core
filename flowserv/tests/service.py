# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to initialize the API service object for test purposes."""

from flowserv.service.api import API
from flowserv.tests.controller import StateEngine

import flowserv.tests.db as db


def init_service(basedir, templatedir, wf_count, gr_count, users=None,
    specfile=None, engine=None
):
    """Initialize a database with wf_count workflows, each having gr_count
    groups. Returns the service API, the workflow controller (engine), and a
    list of (workflow_id, list(group_id, user_id))-pairs.
    """
    # Initialize the database. If no users are given a default user will be
    # created.
    if users is None:
        users = ['0000']
    con = db.init_db(str(basedir), users=users).connect()
    # Create workflow controller abd API service instance
    engine = engine if engine is not None else StateEngine()
    api = API(con=con, engine=engine, basedir=str(basedir))
    # Create two workflows and groups
    workflows = list()
    for w in range(wf_count):
        name = 'W{}'.format(w)
        r = api.workflows().create_workflow(
            name=name,
            sourcedir=templatedir,
            specfile=specfile
        )
        w_id = r['id']
        groups = list()
        for g in range(gr_count):
            # Create two groups for each workflow
            name = 'G{}'.format(g)
            u_id = users[g % len(users)]
            r = api.groups().create_group(
                workflow_id=w_id,
                name=name,
                user_id=u_id
            )
            g_id = r['id']
            groups.append((g_id, u_id))
        workflows.append((w_id, groups))
    return api, engine, workflows
