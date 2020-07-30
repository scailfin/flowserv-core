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

import os
import pytest

from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager

import flowserv.error as err


DIR = os.path.dirname(os.path.realpath(__file__))
# 'Hello World' benchmark directory.
BENCHMARK_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')
# Alternative manifest file with description, instructions. Copies only the
# code/ folder of the template.
ALT_MANIFEST = os.path.join(BENCHMARK_DIR, 'flowserv.alt.yaml')
# Optional instructions file for the 'Hello World' template.
INSTRUCTION_FILE = os.path.join(BENCHMARK_DIR, 'instructions.md')
# Alternative workflow specifications.
TEMPLATE_DIR = os.path.join(DIR, '../../.files/template')
TEMPLATE_WITHOUT_SCHEMA = os.path.join(TEMPLATE_DIR, 'template.json')
error_template = '../../template/alt-validate-error.yaml'
TEMPLATE_WITH_ERROR = os.path.join(TEMPLATE_DIR, 'alt-validate-error.yaml')
TEMPLATE_TOPTAGGER = os.path.join(BENCHMARK_DIR, '../top-tagger.yaml')


"""Fake template for descriptor initialization."""
TEMPLATE = dict({'A': 1})


def test_create_workflow(database, tmpdir):
    """Test creating workflows with different levels of detail."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    # -- Add workflow with minimal information --------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(source=BENCHMARK_DIR)
        assert wf.name == 'Hello World'
        assert wf.description is None
        assert wf.instructions is None
        template = wf.get_template()
        assert template.result_schema is not None
        templatedir = template.sourcedir
        assert os.path.isfile(os.path.join(templatedir, 'code/helloworld.py'))
        assert os.path.isfile(os.path.join(templatedir, 'data/names.txt'))
    # -- Add workflow with user-provided metadata -----------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(
            name='My benchmark',
            description='My description',
            instructions=INSTRUCTION_FILE,
            source=BENCHMARK_DIR
        )
        assert wf.name == 'My benchmark'
        assert wf.description == 'My description'
        assert wf.instructions == '# Hello World'
        template = wf.get_template()
        assert template.result_schema is not None
        templatedir = template.sourcedir
        assert os.path.isfile(os.path.join(templatedir, 'code/helloworld.py'))
        assert os.path.isfile(os.path.join(templatedir, 'data/names.txt'))


def test_create_workflow_with_alt_spec(database, tmpdir):
    """Test creating workflows with alternative specification files."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    # -- Template without schema ----------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(
            source=BENCHMARK_DIR,
            specfile=TEMPLATE_WITHOUT_SCHEMA
        )
        workflow_id = wf.workflow_id
        assert wf.name == 'Hello World'
        template = wf.get_template()
        assert template.result_schema is None
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_id=workflow_id)
        assert wf.name == 'Hello World'
        template = wf.get_template()
        assert template.result_schema is None
    # -- Template with post-processing step -----------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(
            name='Top Tagger',
            source=BENCHMARK_DIR,
            specfile=TEMPLATE_TOPTAGGER
        )
        workflow_id = wf.workflow_id
        assert wf.get_template().postproc_spec is not None
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_id=workflow_id)
        assert wf.get_template().postproc_spec is not None


def test_create_workflow_with_error(database, tmpdir):
    """Error cases when creating a workflow."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    # -- Invalid name ---------------------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        with pytest.raises(err.ConstraintViolationError):
            manager.create_workflow(name=' ', source=BENCHMARK_DIR)
        manager.create_workflow(name='a' * 512, source=BENCHMARK_DIR)
        with pytest.raises(err.ConstraintViolationError):
            manager.create_workflow(name='a' * 513, source=BENCHMARK_DIR)
        # - Invalid template
        with pytest.raises(err.UnknownParameterError):
            manager.create_workflow(
                name='A benchmark',
                source=BENCHMARK_DIR,
                specfile=TEMPLATE_WITH_ERROR
            )


def test_create_workflow_with_alt_manifest(database, tmpdir):
    """Test creating 'Hello World' workflow with a different manifest file."""
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(
            source=BENCHMARK_DIR,
            manifestfile=ALT_MANIFEST
        )
        assert wf.name == 'Hello World'
        assert wf.description == 'Hello World Demo'
        assert wf.instructions == '# Hello World'
        template = wf.get_template()
        assert template.result_schema is not None
        templatedir = template.sourcedir
        assert os.path.isfile(os.path.join(templatedir, 'code/helloworld.py'))
        assert not os.path.isfile(os.path.join(templatedir, 'data/names.txt'))


def test_delete_workflow(database, tmpdir):
    """Test deleting a workflows from the repository."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two workflows.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(name='A', source=BENCHMARK_DIR)
        workflow_1 = wf.workflow_id
        # Ensure that the tample and workflow folder exists for workflow 1.
        templatedir = wf.get_template().sourcedir
        workflowdir = fs.workflow_basedir(workflow_1)
        assert os.path.isdir(templatedir)
        assert os.path.isdir(workflowdir)
        wf = manager.create_workflow(name='B', source=BENCHMARK_DIR)
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
            manager.create_workflow(name='A', source=BENCHMARK_DIR)
        assert dummy_func.count == 101


def test_get_workflow(database, tmpdir):
    """Test retrieving workflows from the repository."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two workflows.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(name='A', source=BENCHMARK_DIR)
        workflow_1 = wf.workflow_id
        wf = manager.create_workflow(
            name='B',
            description='Workflow B',
            source=BENCHMARK_DIR,
            instructions=INSTRUCTION_FILE,
            specfile=TEMPLATE_WITHOUT_SCHEMA
        )
        workflow_2 = wf.workflow_id
    # -- Test getting workflow handles ----------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_1)
        assert wf.name == 'A'
        assert wf.description is None
        assert wf.instructions is None
        template = wf.get_template()
        assert template.result_schema is not None
        wf = manager.get_workflow(workflow_2)
        assert wf.name == 'B'
        assert wf.description == 'Workflow B'
        assert wf.instructions == '# Hello World'
        template = wf.get_template()
        assert template.result_schema is None


def test_list_workflow(database, tmpdir):
    """Test deleting a workflows from the repository."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two workflows.
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        manager.create_workflow(source=BENCHMARK_DIR)
        manager.create_workflow(source=BENCHMARK_DIR)
        manager.create_workflow(source=BENCHMARK_DIR)
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
        wf = manager.create_workflow(name='A', source=BENCHMARK_DIR)
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
        wf = manager.create_workflow(name='A', source=BENCHMARK_DIR)
        workflow_1 = wf.workflow_id
        wf = manager.create_workflow(
            name='My benchmark',
            description='desc',
            instructions=INSTRUCTION_FILE,
            source=BENCHMARK_DIR
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
        manager.create_workflow(name='Workflow', source=BENCHMARK_DIR)
        manager.create_workflow(name='Workflow (2)', source=BENCHMARK_DIR)
    # Creating another workflow with name 'Workflow' will result in a workflow
    # with name 'Workflow (1)' and the 'Workflow (3)'
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(name='Workflow', source=BENCHMARK_DIR)
        assert wf.name == 'Workflow (1)'
        wf = manager.create_workflow(name='Workflow', source=BENCHMARK_DIR)
        assert wf.name == 'Workflow (3)'
