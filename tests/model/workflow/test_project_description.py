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


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/predictor')
INSTRUCTIONS_FILE = os.path.join(TEMPLATE_DIR, 'instructions.txt')
SPEC_FILE = os.path.join(TEMPLATE_DIR, 'predictor.yml')


def test_create_workflow_with_description(wfmanager):
    """Test adding a workflow to the repository with a project description
    file.
    """
    # Initialize the repository
    # Add workflow with minimal information
    wf = wfmanager.create_workflow(sourcedir=TEMPLATE_DIR)
    assert wf.name == 'Dummy'
    assert wf.description == 'Dummy example for test purposes'
    assert wf.instructions == '# How to run'
    template = wfmanager.get_template(wf.workflow_id)
    assert not template.has_schema()
    templatedir = template.sourcedir
    assert not os.path.isfile(os.path.join(templatedir, 'code/dont-copy.py'))
    assert os.path.isfile(os.path.join(templatedir, 'data/input.txt'))
    assert os.path.isfile(os.path.join(templatedir, 'copy-me.txt'))
    assert os.path.isfile(os.path.join(templatedir, 'misc/copy-this.txt'))
    # Override values in description file
    wf = wfmanager.create_workflow(
        sourcedir=TEMPLATE_DIR,
        name='My Name',
        description='My Description',
        instructions=INSTRUCTIONS_FILE,
        specfile=SPEC_FILE
    )
    assert wf.name == 'My Name'
    assert wf.description == 'My Description'
    assert wf.instructions == 'My Instructions'
    assert len(wf.result_schema.columns) == 2
    template = wfmanager.get_template(wf.workflow_id)
    assert template.has_schema()
