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


def test_create_workflow(database, tmpdir):
    """Test creating workflows with different levels of detail."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    # -- Add workflow with minimal information --------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
        assert wf.name == 'A'
        assert wf.description is None
        assert wf.instructions is None
        template = wf.get_template()
        assert template.has_schema()
        templatedir = template.sourcedir
        assert os.path.isfile(os.path.join(templatedir, 'code/helloworld.py'))
        assert os.path.isfile(os.path.join(templatedir, 'data/names.txt'))
    # -- Template without schema ----------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(
            name='My benchmark',
            description='desc',
            instructions=INSTRUCTION_FILE,
            sourcedir=TEMPLATE_DIR,
            specfile=TEMPLATE_WITHOUT_SCHEMA
        )
        workflow_id = wf.workflow_id
        assert wf.name == 'My benchmark'
        assert wf.description == 'desc'
        assert wf.instructions == 'How to run Hello World'
        template = wf.get_template()
        assert not template.has_schema()
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_id=workflow_id)
        assert wf.name == 'My benchmark'
        assert wf.description == 'desc'
        assert wf.instructions == 'How to run Hello World'
        template = wf.get_template()
        assert not template.has_schema()
    # -- Template with post-processing step -----------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(
            name='Top Tagger',
            description='desc',
            instructions=INSTRUCTION_FILE,
            sourcedir=TEMPLATE_DIR,
            specfile=TOPTAGGER_YAML_FILE
        )
        workflow_id = wf.workflow_id
        assert wf.get_template().postproc_spec is not None
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_id=workflow_id)
        assert wf.get_template().postproc_spec is not None
    # -- Error cases ----------------------------------------------------------
    # - Invalid name
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        with pytest.raises(err.ConstraintViolationError):
            manager.create_workflow(name=' ', sourcedir=TEMPLATE_DIR)
        manager.create_workflow(name='a' * 512, sourcedir=TEMPLATE_DIR)
        with pytest.raises(err.ConstraintViolationError):
            manager.create_workflow(name='a' * 513, sourcedir=TEMPLATE_DIR)
        # - Invalid template
        with pytest.raises(err.UnknownParameterError):
            manager.create_workflow(
                name='A benchmark',
                sourcedir=TEMPLATE_DIR,
                specfile=TEMPLATE_WITH_ERROR
            )
        # - Source and repo given
        with pytest.raises(ValueError):
            manager.create_workflow(name='Git', sourcedir='src', repourl='git')
        # - No source given
        with pytest.raises(ValueError):
            manager.create_workflow(name='A benchmark')
        # - Error cloning repository
        with pytest.raises(git.exc.GitCommandError):
            manager.create_workflow(name='A benchmark', repourl='/dev/null')


def test_delete_workflow(database, tmpdir):
    """Test deleting a workflows from the repository."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two workflows.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
        workflow_1 = wf.workflow_id
        # Ensure that the tample and workflow folder exists for workflow 1.
        templatedir = wf.get_template().sourcedir
        workflowdir = fs.workflow_basedir(workflow_1)
        assert os.path.isdir(templatedir)
        assert os.path.isdir(workflowdir)
        wf = manager.create_workflow(name='B', sourcedir=TEMPLATE_DIR)
        workflow_2 = wf.workflow_id
    # -- Test delete first workflow -------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        manager.delete_workflow(workflow_1)
        assert not os.path.isdir(templatedir)
        assert not os.path.isdir(workflowdir)
    with database.session() as session:
        # The second workflow still exists.
        manager = WorkflowManager(session=session, fs=fs)
        manager.get_workflow(workflow_2) is not None
    # -- Deleting the same repository multiple times raises an error ----------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        with pytest.raises(err.UnknownWorkflowError):
            manager.delete_workflow(workflow_id=workflow_1)


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
    # -- Setup ----------------------------------------------------------------
    with database.session() as session:
        dummy_func = DummyIDFunc()
        manager = WorkflowManager(
            session=session,
            fs=WorkflowFileSystem(tmpdir),
            idfunc=dummy_func
        )
        os.makedirs(manager.fs.workflow_basedir('0000'))
        with pytest.raises(RuntimeError):
            manager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
        assert dummy_func.count == 101


def test_get_workflow(database, tmpdir):
    """Test retrieving workflows from the repository."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two workflows.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
        workflow_1 = wf.workflow_id
        wf = manager.create_workflow(name='B', sourcedir=TEMPLATE_DIR)
        workflow_2 = wf.workflow_id
    # -- Test getting workflow handles ----------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_1)
        assert wf.name == 'A'
        assert wf.name == 'A'
        assert wf.description is None
        assert wf.instructions is None
        template = wf.get_template()
        assert template.has_schema()
        wf = manager.get_workflow(workflow_2)
        assert wf.name == 'B'


def test_list_workflow(database, tmpdir):
    """Test deleting a workflows from the repository."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two workflows.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        manager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
        manager.create_workflow(name='B', sourcedir=TEMPLATE_DIR)
        manager.create_workflow(name='C', sourcedir=TEMPLATE_DIR)
    # -- Test list workflows --------------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        workflows = manager.list_workflows()
        assert len(workflows) == 3


def test_update_workflow_description(database, tmpdir):
    """Test updating the name and description of a workflow."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one workflow without description and instructions.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        # Initialize the repository
        wf = manager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
        workflow_id = wf.workflow_id
    # -- Test update description and instruction text -------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.update_workflow(
            workflow_id=workflow_id,
            description='The description',
            instructions='The instructions'
        )
        assert wf.description == 'The description'
        assert wf.instructions == 'The instructions'
    # -- Test no update -------------------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.update_workflow(workflow_id=workflow_id)
        assert wf.name == 'A'
        assert wf.description == 'The description'
        assert wf.instructions == 'The instructions'
    # -- Test update all three properties -------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.update_workflow(
            workflow_id=workflow_id,
            name='A name',
            description='A description',
            instructions='Some instructions'
        )
        assert wf.name == 'A name'
        assert wf.description == 'A description'
        assert wf.instructions == 'Some instructions'


def test_update_workflow_name(database, tmpdir):
    """Test updating workflow names."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two workflow templates. Workflow 1 does not have a description
    # and instructions while workflow 2 has.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        # Initialize the repository
        wf = manager.create_workflow(name='A', sourcedir=TEMPLATE_DIR)
        workflow_1 = wf.workflow_id
        wf = manager.create_workflow(
            name='My benchmark',
            description='desc',
            instructions=INSTRUCTION_FILE,
            sourcedir=TEMPLATE_DIR
        )
        workflow_2 = wf.workflow_id
    # -- Test update workflow name --------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.update_workflow(workflow_id=workflow_1, name='B')
        assert wf.name == 'B'
        # It is possible to change the name to an existing name only if it is
        # the same workflow.
        wf = manager.update_workflow(
            workflow_id=workflow_2,
            name='My benchmark'
        )
        assert wf.name == 'My benchmark'
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        # Cannot change name to existing name.
        with pytest.raises(err.ConstraintViolationError):
            manager.update_workflow(workflow_id=workflow_2, name='B')


def test_workflow_name(database, tmpdir):
    """Test creating workflows with existing names."""
    # -- Setup ----------------------------------------------------------------
    # Initialize the repository. Create two workflows, one with name 'Workflow'
    # and the other with name 'Workflow (2)'
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        manager.create_workflow(name='Workflow', sourcedir=TEMPLATE_DIR)
        manager.create_workflow(name='Workflow (2)', sourcedir=TEMPLATE_DIR)
    # Creating another workflow with name 'Workflow' will result in a workflow
    # with name 'Workflow (1)' and the 'Workflow (3)'
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(name='Workflow', sourcedir=TEMPLATE_DIR)
        assert wf.name == 'Workflow (1)'
        wf = manager.create_workflow(name='Workflow', sourcedir=TEMPLATE_DIR)
        assert wf.name == 'Workflow (3)'
    # Test using a repository name
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        pmeta = dict()
        helper.get_unique_name(
            projectmeta=pmeta,
            sourcedir=None,
            repourl='https://github.com/scailfin/rob-demo-hello-world.git',
            existing_names=[w.name for w in manager.list_workflows()]
        )
        assert pmeta[helper.NAME] == 'Rob Demo Hello World'
