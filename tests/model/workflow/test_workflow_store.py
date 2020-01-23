# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for basic functionality of the workflow repository. Contains
tests for creating, retrieving, updating, listing, and deleting workflows
in the repository.
"""

import os
import pytest

from flowserv.model.template.repo import TemplateRepository
from flowserv.model.workflow.repo import WorkflowRepository

import flowserv.core.error as err
import flowserv.tests.db as db


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')
TEMPLATE_JSON = '../../.files/template/template.json'
TEMPLATE_WITHOUT_SCHEMA = os.path.join(DIR, TEMPLATE_JSON)
TOP_TAGGER_YAML = '../../.files/benchmark/top-tagger.yaml'
TOPTAGGER_YAML_FILE = os.path.join(DIR, TOP_TAGGER_YAML)


"""Fake template for descriptor initialization."""
TEMPLATE = dict({'A': 1})


def init(basedir):
    """Create empty database. Return a test instance of the workflow
    repository and a connector to the database.
    """
    connector = db.init_db(str(basedir))
    repo = WorkflowRepository(
        con=connector.connect(),
        template_repo=TemplateRepository(
            basedir=os.path.join(basedir, 'templates')
        ),
        basedir=os.path.join(basedir, 'workflows')
    )
    return repo, connector


def test_add_workflow(tmpdir):
    """Test adding, retrieving and listing workflows."""
    # Initialize the repository
    repo, connector = init(tmpdir)
    # Add workflow with minimal information
    wf1 = repo.add_workflow(name='A', sourcedir=TEMPLATE_DIR)
    assert wf1.name == 'A'
    assert not wf1.has_description()
    assert wf1.get_description() == ''
    assert not wf1.has_instructions()
    assert wf1.get_instructions() == ''
    assert wf1.get_template().has_schema()
    # Get workflow from repository
    wf1 = repo.get_workflow(wf1.identifier)
    assert wf1.name == 'A'
    assert not wf1.has_description()
    assert wf1.get_description() == ''
    assert not wf1.has_instructions()
    assert wf1.get_instructions() == ''
    assert wf1.get_template().has_schema()
    workflows = repo.list_workflows()
    assert len(workflows) == 1
    assert wf1.identifier in [w.identifier for w in workflows]
    # Template without schema
    wf2 = repo.add_workflow(
        name='My benchmark',
        description='desc',
        instructions='instr',
        sourcedir=TEMPLATE_DIR,
        specfile=TEMPLATE_WITHOUT_SCHEMA
    )
    assert wf2.name == 'My benchmark'
    assert wf2.has_description()
    assert wf2.get_description() == 'desc'
    assert wf2.has_instructions()
    assert wf2.get_instructions() == 'instr'
    assert not wf2.get_template().has_schema()
    wf2 = repo.get_workflow(wf2.identifier)
    assert wf2.name == 'My benchmark'
    assert wf2.has_description()
    assert wf2.get_description() == 'desc'
    assert wf2.has_instructions()
    assert wf2.get_instructions() == 'instr'
    assert not wf2.get_template().has_schema()
    workflows = repo.list_workflows()
    assert len(workflows) == 2
    assert wf1.identifier in [w.identifier for w in workflows]
    assert wf2.identifier in [w.identifier for w in workflows]
    # Template with post-processing step
    wf3 = repo.add_workflow(
        name='Top Tagger',
        description='desc',
        instructions='instr',
        sourcedir=TEMPLATE_DIR,
        specfile=TOPTAGGER_YAML_FILE
    )
    assert wf3.get_template().postproc_spec is not None
    wf3 = repo.get_workflow(wf3.identifier)
    assert wf3.get_template().postproc_spec is not None
    workflows = repo.list_workflows()
    assert len(workflows) == 3
    # Test error conditions
    # - Missing name
    with pytest.raises(err.ConstraintViolationError):
        repo.add_workflow(name=None, sourcedir=TEMPLATE_DIR)
    with pytest.raises(err.ConstraintViolationError):
        repo.add_workflow(name=' ', sourcedir=TEMPLATE_DIR)
    # - Invalid name
    repo.add_workflow(name='a' * 512, sourcedir=TEMPLATE_DIR)
    with pytest.raises(err.ConstraintViolationError):
        repo.add_workflow(name='a' * 513, sourcedir=TEMPLATE_DIR)
    # - Duplicate name
    with pytest.raises(err.ConstraintViolationError):
        repo.add_workflow(name='My benchmark', sourcedir=TEMPLATE_DIR)
    # - No source given
    with pytest.raises(ValueError):
        repo.add_workflow(name='A benchmark')


def test_delete_workflow(tmpdir):
    """Test deleting a workflows from the repository."""
    # Initialize the repository
    repo, connector = init(tmpdir)
    wf1 = repo.add_workflow(name='A', sourcedir=TEMPLATE_DIR)
    wf2 = repo.add_workflow(name='B', sourcedir=TEMPLATE_DIR)
    wf3 = repo.add_workflow(name='C', sourcedir=TEMPLATE_DIR)
    workflows = repo.list_workflows()
    assert len(workflows) == 3
    assert wf1.identifier in [w.identifier for w in workflows]
    assert wf2.identifier in [w.identifier for w in workflows]
    assert wf3.identifier in [w.identifier for w in workflows]
    # Ensure that the tample and workflow folder exists for wf1 prior to
    # deletion and that both folders are deleted correctly
    templatedir = os.path.join(repo.template_repo.basedir, wf1.identifier)
    workflowdir = repo.fs.workflow_basedir(wf1.identifier)
    assert os.path.isdir(templatedir)
    assert os.path.isdir(workflowdir)
    repo.delete_workflow(wf1.identifier)
    assert not os.path.isdir(templatedir)
    assert not os.path.isdir(workflowdir)
    workflows = repo.list_workflows()
    assert len(workflows) == 2
    assert wf1.identifier not in [w.identifier for w in workflows]
    assert wf2.identifier in [w.identifier for w in workflows]
    assert wf3.identifier in [w.identifier for w in workflows]
    # Deleting the same repository multiple times raises an error
    repo.delete_workflow(wf2.identifier)
    with pytest.raises(err.UnknownWorkflowError):
        repo.delete_workflow(wf2.identifier)
    workflows = repo.list_workflows()
    assert len(workflows) == 1
    assert wf1.identifier not in [w.identifier for w in workflows]
    assert wf2.identifier not in [w.identifier for w in workflows]
    assert wf3.identifier in [w.identifier for w in workflows]


def test_get_workflow(tmpdir):
    """Test retrieving workflows from the repository."""
    # Initialize the repository
    repo, connector = init(tmpdir)
    wf1 = repo.add_workflow(name='A', sourcedir=TEMPLATE_DIR)
    wf2 = repo.add_workflow(name='B', sourcedir=TEMPLATE_DIR)
    # Re-connect to the repository
    repo = WorkflowRepository(
        con=connector.connect(),
        template_repo=TemplateRepository(
            basedir=os.path.join(str(tmpdir), 'templates')
        ),
        basedir=os.path.join(str(tmpdir), 'workflows')
    )
    wf_reload = repo.get_workflow(wf1.identifier)
    assert wf1.identifier == wf_reload.identifier
    assert wf1.name == wf_reload.name
    # Delete the workflow
    repo.delete_workflow(wf1.identifier)
    with pytest.raises(err.UnknownWorkflowError):
        repo.get_workflow(wf1.identifier)
    # The second workflow still exists
    wf_reload = repo.get_workflow(wf2.identifier)
    assert wf2.identifier == wf_reload.identifier
    assert wf2.name == wf_reload.name


def test_update_workflow(tmpdir):
    """Test updating workflow properties."""
    # Initialize the repository
    repo, connector = init(tmpdir)
    wf1 = repo.add_workflow(name='A', sourcedir=TEMPLATE_DIR)
    wf2 = repo.add_workflow(
        name='My benchmark',
        description='desc',
        instructions='instr',
        sourcedir=TEMPLATE_DIR
    )
    # Update the name of the first workflow. It is possible to change the
    # name to an existing name only if it is the same workflow
    wf1 = repo.update_workflow(workflow_id=wf1.identifier, name='B')
    assert wf1.name == 'B'
    wf1 = repo.update_workflow(workflow_id=wf1.identifier, name='B')
    assert wf1.name == 'B'
    with pytest.raises(err.ConstraintViolationError):
        repo.update_workflow(
            workflow_id=wf1.identifier,
            name='My benchmark'
        )
    # Update description and instructions
    wf1 = repo.update_workflow(
        workflow_id=wf1.identifier,
        description='My description',
        instructions='My instructions'
    )
    assert wf1.name == 'B'
    assert wf1.description == 'My description'
    assert wf1.instructions == 'My instructions'
    wf2 = repo.update_workflow(
        workflow_id=wf2.identifier,
        name='The name',
        description='The description',
        instructions='The instructions'
    )
    assert wf2.name == 'The name'
    assert wf2.description == 'The description'
    assert wf2.instructions == 'The instructions'
    # Do nothing
    wf1 = repo.update_workflow(workflow_id=wf1.identifier)
    assert wf1.name == 'B'
    assert wf1.description == 'My description'
    assert wf1.instructions == 'My instructions'
    wf2 = repo.update_workflow(workflow_id=wf2.identifier)
    assert wf2.name == 'The name'
    assert wf2.description == 'The description'
    assert wf2.instructions == 'The instructions'