# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for adding a workflow template to a repository using a project
description file.
"""

import os

from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.repo import WorkflowRepository

import flowserv.core.util as util
import flowserv.tests.db as db


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/predictor')
INSTRUCTIONS_FILE = os.path.join(TEMPLATE_DIR, 'instructions.txt')
SPEC_FILE = os.path.join(TEMPLATE_DIR, 'predictor.yml')


def init(basedir):
    """Create empty database. Return a test instance of the workflow
    repository and a connector to the database.
    """
    connector = db.init_db(str(basedir))
    repodir = util.create_dir(os.path.join(str(basedir), 'workflows'))
    repo = WorkflowRepository(
        con=connector.connect(),
        fs=WorkflowFileSystem(repodir)
    )
    return repo, connector


def test_create_workflow_with_description(tmpdir):
    """Test adding a workflow to the repository with a project description
    file.
    """
    # Initialize the repository
    repo, connector = init(tmpdir)
    # Add workflow with minimal information
    wf = repo.create_workflow(sourcedir=TEMPLATE_DIR)
    assert wf.name == 'Dummy'
    assert wf.get_description() == 'Dummy example for test purposes'
    assert wf.get_instructions() == '# How to run'
    assert not wf.get_template().has_schema()
    templatedir = wf.get_template().sourcedir
    assert not os.path.isfile(os.path.join(templatedir, 'code/dont-copy.py'))
    assert os.path.isfile(os.path.join(templatedir, 'data/input.txt'))
    assert os.path.isfile(os.path.join(templatedir, 'copy-me.txt'))
    # Override values in description file
    wf = repo.create_workflow(
        sourcedir=TEMPLATE_DIR,
        name='My Name',
        description='My Description',
        instructions=INSTRUCTIONS_FILE,
        specfile=SPEC_FILE
    )
    assert wf.name == 'My Name'
    assert wf.get_description() == 'My Description'
    assert wf.get_instructions() == 'My Instructions'
    assert wf.get_template().has_schema()
