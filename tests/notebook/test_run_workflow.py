# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API methods."""

import os
import pytest
import shutil

from flowserv.model.workflow.manager import clone
from flowserv.model.workflow.repository import WorkflowRepository
from flowserv.tests.workflow import (
    clone_helloworld, prepare_postproc_data, run_postproc_workflow,
    run_workflow, INPUTFILE
)
from flowserv.service.postproc.client import Runs
import flowserv.error as err


def test_running_workflow_template(tmpdir):
    """Test helper function for running a workflow template."""
    templatedir = os.path.join(tmpdir, 'template')
    clone_helloworld(targetdir=templatedir)
    templatefile = os.path.join(templatedir, 'benchmark.yaml')
    namesfile = os.path.join(templatedir, 'data/names.txt')
    repo = WorkflowRepository(templates=[])
    # -- Run the workflow -----------------------------------------------------
    with clone(templatedir, repository=repo) as (workflowdir, manifestpath):
        # Run with all parameters given.
        args = {
            'greeting': 'Hey there',
            'sleeptime': 2,
            'names': INPUTFILE(namesfile)
        }
        rundir1 = os.path.join(tmpdir, 'run1')
        state = run_workflow(workflowdir, arguments=args, rundir=rundir1)
        assert state.is_success()
        # Run with default greeting.
        args = {
            'sleeptime': 2,
            'names': INPUTFILE(namesfile)
        }
        rundir2 = os.path.join(tmpdir, 'run2')
        state = run_workflow(workflowdir, arguments=args, rundir=rundir2)
        assert state.is_success()
        # Error when mandatory parameter is missing.
        args = {'sleeptime': 2}
        rundir3 = os.path.join(tmpdir, 'run3')
        with pytest.raises(err.MissingArgumentError):
            run_workflow(workflowdir, arguments=args, rundir=rundir3)
    # -- Post-processing data -------------------------------------------------
    postprocdata = prepare_postproc_data(templatefile, runs=[rundir1, rundir2])
    runs = Runs(postprocdata)
    assert len(runs) == 2
    shutil.rmtree(postprocdata)
    # -- Run post-processing workflow -----------------------------------------
    postprocdir = os.path.join(tmpdir, 'postproc')
    state = run_postproc_workflow(
        sourcedir=templatedir,
        runs=[rundir1, rundir2],
        rundir=postprocdir
    )
    assert state.is_success()
    assert state.files == ['results/ngrams.csv']
