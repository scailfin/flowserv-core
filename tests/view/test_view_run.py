# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow run resources view."""

import os

from flowserv.config import Config
from flowserv.model.files.fs import FileSystemStore
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.run import RunManager
from flowserv.view.run import RunSerializer
from flowserv.view.validate import validator

import flowserv.tests.model as model
import flowserv.util as util
import flowserv.view.run as labels


def test_run_serialization(database, tmpdir):
    """Test serialization of run handles and run listings."""
    config = Config().basedir(tmpdir)
    view = RunSerializer()
    fs = FileSystemStore(config)
    # Setup temporary run folder.
    tmprundir = os.path.join(tmpdir, 'tmprun')
    tmpresultsdir = os.path.join(tmprundir, 'run', 'results')
    os.makedirs(tmprundir)
    os.makedirs(tmpresultsdir)
    f1 = os.path.join(tmprundir, 'A.json')
    util.write_object(f1, {'A': 1})
    # Create runs.
    with database.session() as session:
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
        # Create successful run.
        groups = WorkflowGroupManager(session=session, fs=fs)
        runs = RunManager(session=session, fs=fs)
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
        state = run.state()
        runs.update_run(
            run_id,
            state.start().success(files=['A.json', 'run/results/B.json']),
            rundir=tmprundir
        )
        run = runs.get_run(run_id)
        doc = view.run_handle(run)
        validator('RunHandle').validate(doc)
        # Create error run.
        run = runs.create_run(group=groups.get_group(group_id))
        run_id = run.run_id
        state = run.state()
        runs.update_run(run_id=run_id, state=state)
        messages = ['There', 'were', 'many errors']
        runs.update_run(run_id=run_id, state=state.error(messages))
        run = runs.get_run(run_id)
        doc = view.run_handle(run)
        validator('RunHandle').validate(doc)
        # Validate run listing.
        doc = view.run_listing(runs=runs.list_runs(group_id), group_id=group_id)
        validator('RunListing').validate(doc)
        assert len(doc[labels.RUN_LIST]) == 2
