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

from flowserv.model.workflow.manager import clone
from flowserv.model.workflow.repository import WorkflowRepository

from flowserv.tests.workflow import run_workflow, INPUTFILE, GITHUB_HELLOWORLD

import flowserv.error as err

DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')
NAMES_FILE = os.path.join(TEMPLATE_DIR, 'data/names.txt')


def test_running_workflow_template(tmpdir):
    """Test helper function for running a workflow template."""
    repo = WorkflowRepository(templates=[])
    with clone(GITHUB_HELLOWORLD, repository=repo) as workflowdir:
        # Run with all parameters given.
        args = {
            'greeting': 'Hey there',
            'sleeptime': 2,
            'names': INPUTFILE(NAMES_FILE)
        }
        rundir = os.path.join(tmpdir, 'run1')
        state = run_workflow(workflowdir, arguments=args, rundir=rundir)
        assert state.is_success()
        # Run with default greeting.
        args = {
            'sleeptime': 2,
            'names': INPUTFILE(NAMES_FILE)
        }
        rundir = os.path.join(tmpdir, 'run2')
        state = run_workflow(workflowdir, arguments=args, rundir=rundir)
        assert state.is_success()
        # Error when mandatory parameter is missing.
        args = {'sleeptime': 2}
        rundir = os.path.join(tmpdir, 'run3')
        with pytest.raises(err.MissingArgumentError):
            run_workflow(workflowdir, arguments=args, rundir=rundir)
