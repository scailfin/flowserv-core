# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for adding a workflow template to a repository using a flowserv
configuration file.
"""

import os

from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/predictor')
INSTRUCTIONS_FILE = os.path.join(TEMPLATE_DIR, 'instructions.txt')
SPEC_FILE = os.path.join(TEMPLATE_DIR, 'predictor.yml')


def test_create_workflow_with_config(database, tmpdir):
    """Test adding a workflow to the repository from a source folder that
    contains a flowserv.yaml file.
    """
    fs = WorkflowFileSystem(tmpdir)
    # -- Test create workflow from config file --------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(sourcedir=TEMPLATE_DIR)
        assert wf.name == 'Dummy'
        assert wf.description == 'Dummy example for test purposes'
        assert wf.instructions == '# How to run'
        template = wf.get_template()
        assert template.result_schema is None
        templatedir = template.sourcedir
        assert wf.parameters.get('code').target is None
    # -- Ensure that only those files have been copied that are listed in the
    #    config file ----------------------------------------------------------
    assert not os.path.isfile(os.path.join(templatedir, 'code/dont-copy.py'))
    assert os.path.isfile(os.path.join(templatedir, 'data/input.txt'))
    assert os.path.isfile(os.path.join(templatedir, 'copy-me.txt'))
    assert os.path.isfile(os.path.join(templatedir, 'misc/copy-this.txt'))
    # -- Test override values in description file
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(sourcedir=TEMPLATE_DIR)
        wf = manager.create_workflow(
            sourcedir=TEMPLATE_DIR,
            name='My Name',
            description='My Description',
            instructions=INSTRUCTIONS_FILE,
            specfile=SPEC_FILE
        )
        assert wf.name == 'My Name'
        assert wf.description == 'My Description'
        assert wf.instructions == 'My Instructions'
        assert wf.parameters.get('code').target == 'code/script.py'
        assert len(wf.result_schema.columns) == 2
        template = wf.get_template()
