# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for basic functionality of the workflow repository. Contains
tests for creating, retrieving, updating, listing, and deleting workflows
in the repository.
"""

import git
import os
import pytest

from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager

import flowserv.error as err
import flowserv.model.workflow.manager as helper


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')
INSTRUCTION_FILE = os.path.join(DIR, '../../.files/benchmark/instructions.txt')
json_template = '../../.files/template/template.json'
TEMPLATE_WITHOUT_SCHEMA = os.path.join(DIR, json_template)
error_template = '../../.files/template/alt-validate-error.yaml'
TEMPLATE_WITH_ERROR = os.path.join(DIR, error_template)
TOP_TAGGER_YAML = '../../.files/benchmark/top-tagger.yaml'
TOPTAGGER_YAML_FILE = os.path.join(DIR, TOP_TAGGER_YAML)


"""Fake template for descriptor initialization."""
TEMPLATE = dict({'A': 1})


def test_create_workflow(wfmanager):
    """Test adding, retrieving and listing workflows."""
    # Initialize the repository
    # Add workflow with minimal information
    wf1 = wfmanager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
    assert wf1.name == 'A'
    assert wf1.description is None
    assert wf1.instructions is None
    template = wfmanager.get_template(wf1.workflow_id)
    assert template.has_schema()
    templatedir = template.sourcedir
    assert os.path.isfile(os.path.join(templatedir, 'code/helloworld.py'))
    assert os.path.isfile(os.path.join(templatedir, 'data/names.txt'))
    # Get workflow from repository
    wf1 = wfmanager.get_workflow(wf1.workflow_id)
    assert wf1.name == 'A'
    assert wf1.description is None
    assert wf1.instructions is None
    template = wfmanager.get_template(wf1.workflow_id)
    assert template.has_schema()
    workflows = wfmanager.list_workflows()
    assert len(workflows) == 1
    assert wf1.workflow_id in [w.workflow_id for w in workflows]
    templatedir = wfmanager.get_template(wf1.workflow_id).sourcedir
    assert os.path.isfile(os.path.join(templatedir, 'code/helloworld.py'))
    assert os.path.isfile(os.path.join(templatedir, 'data/names.txt'))
    # Template without schema
    wf2 = wfmanager.create_workflow(
        name='My benchmark',
        description='desc',
        instructions=INSTRUCTION_FILE,
        sourcedir=TEMPLATE_DIR,
        specfile=TEMPLATE_WITHOUT_SCHEMA
    )
    assert wf2.name == 'My benchmark'
    assert wf2.description == 'desc'
    assert wf2.instructions == 'How to run Hello World'
    template = wfmanager.get_template(wf2.workflow_id)
    assert not template.has_schema()
    wf2 = wfmanager.get_workflow(wf2.workflow_id)
    assert wf2.name == 'My benchmark'
    assert wf2.description == 'desc'
    assert wf2.instructions == 'How to run Hello World'
    template = wfmanager.get_template(wf2.workflow_id)
    assert not template.has_schema()
    workflows = wfmanager.list_workflows()
    assert len(workflows) == 2
    assert wf1.workflow_id in [w.workflow_id for w in workflows]
    assert wf2.workflow_id in [w.workflow_id for w in workflows]
    # Template with post-processing step
    wf3 = wfmanager.create_workflow(
        name='Top Tagger',
        description='desc',
        instructions=INSTRUCTION_FILE,
        sourcedir=TEMPLATE_DIR,
        specfile=TOPTAGGER_YAML_FILE
    )
    assert wfmanager.get_template(wf3.workflow_id).postproc_spec is not None
    wf3 = wfmanager.get_workflow(wf3.workflow_id)
    assert wfmanager.get_template(wf3.workflow_id).postproc_spec is not None
    workflows = wfmanager.list_workflows()
    assert len(workflows) == 3
    # - Missing name
    wf = wfmanager.create_workflow(name=None, sourcedir=TEMPLATE_DIR)
    assert wf.name == 'Helloworld'
    wf = wfmanager.create_workflow(name='My benchmark', sourcedir=TEMPLATE_DIR)
    assert wf.name == 'My benchmark (1)'
    # Test error conditions
    # - Invalid name
    with pytest.raises(err.ConstraintViolationError):
        wfmanager.create_workflow(name=' ', sourcedir=TEMPLATE_DIR)
    wfmanager.create_workflow(name='a' * 512, sourcedir=TEMPLATE_DIR)
    with pytest.raises(err.ConstraintViolationError):
        wfmanager.create_workflow(name='a' * 513, sourcedir=TEMPLATE_DIR)
    # - Invalid template
    with pytest.raises(err.UnknownParameterError):
        wfmanager.create_workflow(
            name='A benchmark',
            sourcedir=TEMPLATE_DIR,
            specfile=TEMPLATE_WITH_ERROR
        )
    # - Source and repo given
    with pytest.raises(ValueError):
        wfmanager.create_workflow(name='Git', sourcedir='src', repourl='git')
    # - No source given
    with pytest.raises(ValueError):
        wfmanager.create_workflow(name='A benchmark')
    # - Error cloning repository
    with pytest.raises(git.exc.GitCommandError):
        wfmanager.create_workflow(name='A benchmark', repourl='/dev/null')


def test_delete_workflow(wfmanager):
    """Test deleting a workflows from the repository."""
    # Initialize the repository
    wf1 = wfmanager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
    wf2 = wfmanager.create_workflow(name='B', sourcedir=TEMPLATE_DIR)
    wf3 = wfmanager.create_workflow(name='C', sourcedir=TEMPLATE_DIR)
    workflows = wfmanager.list_workflows()
    assert len(workflows) == 3
    assert wf1.workflow_id in [w.workflow_id for w in workflows]
    assert wf2.workflow_id in [w.workflow_id for w in workflows]
    assert wf3.workflow_id in [w.workflow_id for w in workflows]
    # Ensure that the tample and workflow folder exists for wf1 prior to
    # deletion and that both folders are deleted correctly
    templatedir = wfmanager.get_template(wf1.workflow_id).sourcedir
    workflowdir = wfmanager.fs.workflow_basedir(wf1.workflow_id)
    assert os.path.isdir(templatedir)
    assert os.path.isdir(workflowdir)
    wfmanager.delete_workflow(wf1.workflow_id)
    assert not os.path.isdir(templatedir)
    assert not os.path.isdir(workflowdir)
    workflows = wfmanager.list_workflows()
    assert len(workflows) == 2
    assert wf1.workflow_id not in [w.workflow_id for w in workflows]
    assert wf2.workflow_id in [w.workflow_id for w in workflows]
    assert wf3.workflow_id in [w.workflow_id for w in workflows]
    # Deleting the same repository multiple times raises an error
    wfmanager.delete_workflow(wf2.workflow_id)
    with pytest.raises(err.UnknownWorkflowError):
        wfmanager.delete_workflow(wf2.workflow_id)
    workflows = wfmanager.list_workflows()
    assert len(workflows) == 1
    assert wf1.workflow_id not in [w.workflow_id for w in workflows]
    assert wf2.workflow_id not in [w.workflow_id for w in workflows]
    assert wf3.workflow_id in [w.workflow_id for w in workflows]


def test_error_for_id_func(database, tmpdir):
    """Error when the id function cannot return unique folder identifier.
    """
    # -- Helper class ---------------------------------------------------------
    class DummyIDFunc():
        """Dummy id function."""
        def __init__(self):
            self.count = 0

        def __call__(self):
            self.count += 1
            return '0000'
    # initialize a repository with the dummy ID function
    dummy_func = DummyIDFunc()
    repo = WorkflowManager(
        db=database,
        fs=WorkflowFileSystem(tmpdir),
        idfunc=dummy_func
    )
    os.makedirs(repo.fs.workflow_basedir('0000'))
    with pytest.raises(RuntimeError):
        repo.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
    assert dummy_func.count == 101


def test_get_workflow(wfmanager):
    """Test retrieving workflows from the repository."""
    # Initialize the repository
    wf1 = wfmanager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
    wf2 = wfmanager.create_workflow(name='B', sourcedir=TEMPLATE_DIR)
    wf_reload = wfmanager.get_workflow(wf1.workflow_id)
    assert wf1.workflow_id == wf_reload.workflow_id
    assert wf1.name == wf_reload.name
    # Delete the workflow
    wfmanager.delete_workflow(wf1.workflow_id)
    with pytest.raises(err.UnknownWorkflowError):
        wfmanager.get_workflow(wf1.workflow_id)
    # The second workflow still exists
    wf_reload = wfmanager.get_workflow(wf2.workflow_id)
    assert wf2.workflow_id == wf_reload.workflow_id
    assert wf2.name == wf_reload.name


def test_update_workflow(wfmanager):
    """Test updating workflow properties."""
    # Initialize the repository
    wf1 = wfmanager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
    wf2 = wfmanager.create_workflow(
        name='My benchmark',
        description='desc',
        instructions=INSTRUCTION_FILE,
        sourcedir=TEMPLATE_DIR
    )
    # Update the name of the first workflow. It is possible to change the
    # name to an existing name only if it is the same workflow
    wf1 = wfmanager.update_workflow(workflow_id=wf1.workflow_id, name='B')
    assert wf1.name == 'B'
    wf1 = wfmanager.update_workflow(workflow_id=wf1.workflow_id, name='B')
    assert wf1.name == 'B'
    with pytest.raises(err.ConstraintViolationError):
        wfmanager.update_workflow(
            workflow_id=wf1.workflow_id,
            name='My benchmark'
        )
    # Update description and instructions
    wf1 = wfmanager.update_workflow(
        workflow_id=wf1.workflow_id,
        description='My description',
        instructions='My instructions'
    )
    assert wf1.name == 'B'
    assert wf1.description == 'My description'
    assert wf1.instructions == 'My instructions'
    wf2 = wfmanager.update_workflow(
        workflow_id=wf2.workflow_id,
        name='The name',
        description='The description',
        instructions='The instructions'
    )
    assert wf2.name == 'The name'
    assert wf2.description == 'The description'
    assert wf2.instructions == 'The instructions'
    # Do nothing
    wf1 = wfmanager.update_workflow(workflow_id=wf1.workflow_id)
    assert wf1.name == 'B'
    assert wf1.description == 'My description'
    assert wf1.instructions == 'My instructions'
    wf2 = wfmanager.update_workflow(workflow_id=wf2.workflow_id)
    assert wf2.name == 'The name'
    assert wf2.description == 'The description'
    assert wf2.instructions == 'The instructions'


def test_workflow_name(wfmanager):
    """Test creating workflows with existing names."""
    # Initialize the repository. Create two workflows, one with name 'Workflow'
    # and the other with name 'Workflow (2)'
    wfmanager.create_workflow(name='Workflow', sourcedir=TEMPLATE_DIR)
    wfmanager.create_workflow(name='Workflow (2)', sourcedir=TEMPLATE_DIR)
    # Creating another workflow with name 'Workflow' will result in a workflow
    # with name 'Workflow (1)' and the 'Workflow (3)'
    wf = wfmanager.create_workflow(name='Workflow', sourcedir=TEMPLATE_DIR)
    assert wf.name == 'Workflow (1)'
    wf = wfmanager.create_workflow(name='Workflow', sourcedir=TEMPLATE_DIR)
    assert wf.name == 'Workflow (3)'
    # Test using a repository name
    pmeta = dict()
    helper.get_unique_name(
        projectmeta=pmeta,
        sourcedir=None,
        repourl='https://github.com/scailfin/rob-demo-hello-world.git',
        existing_names=[w.name for w in wfmanager.list_workflows()]
    )
    assert pmeta[helper.NAME] == 'Rob Demo Hello World'
