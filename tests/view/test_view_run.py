# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow run resources view."""

import os

from flowserv.model.files import io_file
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.run import RunManager
from flowserv.view.run import RunSerializer
from flowserv.view.validate import validator
from flowserv.volume.fs import FileSystemStorage

import flowserv.tests.model as model
import flowserv.view.run as labels


def test_run_serialization(database, tmpdir):
    """Test serialization of run handles and run listings."""
    view = RunSerializer()
    fs = FileSystemStorage(basedir=tmpdir)
    # Setup temporary run folder.
    runfs = FileSystemStorage(basedir=os.path.join(tmpdir, 'tmprun'))
    runfs.store(file=io_file({'A': 1}), dst='A.json')
    runfs.store(file=io_file({'B': 2}), dst='results/B.json')
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
            state.start().success(files=['A.json', 'results/B.json']),
            runstore=runfs
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
        doc = view.run_listing(runs=runs.list_runs(group_id))
        validator('RunListing').validate(doc)
        assert len(doc[labels.RUN_LIST]) == 2
