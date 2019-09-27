# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the abstract workflow engine and the default synchronous
engine implementation.
"""

import os
import pytest

from robcore.io.files import FileHandle
from robcore.model.template.base import WorkflowTemplate
from robcore.model.template.parameter.value import TemplateArgument
from robcore.model.workflow.sync import SyncWorkflowEngine

import robcore.error as err
import robcore.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/template')
# Workflow templates
TEMPLATE_HELLOWORLD = os.path.join(TEMPLATE_DIR, './hello-world.yaml')
TEMPLATE_WITH_INVALID_CMD = os.path.join(TEMPLATE_DIR, './template-invalid-cmd.yaml')
TEMPLATE_WITH_MISSING_FILE = os.path.join(TEMPLATE_DIR, './template-missing-file.yaml')
# Input files
NAMES_FILE = os.path.join(TEMPLATE_DIR, './inputs/short-names.txt')
UNKNOWN_FILE = os.path.join(TEMPLATE_DIR, './tmp/no/file/here')


class TestSynchronousWorkflowEngine(object):
    """Unit test for the synchronous workflow engine."""
    def test_run_helloworld(self, tmpdir):
        """Execute the helloworld example."""
        # Read the workflow template
        doc = util.read_object(filename=TEMPLATE_HELLOWORLD)
        template = WorkflowTemplate.from_dict(doc)
        # Set the template argument values
        arguments = {
            'names': TemplateArgument(
                parameter=template.get_parameter('names'),
                value=FileHandle(NAMES_FILE)
            ),
            'sleeptime': TemplateArgument(
                parameter=template.get_parameter('sleeptime'),
                value=3
            )
        }
        # Run the workflow
        engine = SyncWorkflowEngine(str(tmpdir))
        run_id = util.get_short_identifier()
        state = engine.exec_workflow(
            run_id=run_id,
            template=template,
            source_dir=TEMPLATE_DIR,
            arguments=arguments
        )
        # For completeness. Cancel run should have no effect
        engine.cancel_run(run_id)
        # Expect the result to be success
        assert state.is_success()
        # The base directory for the engine will contain the run state file and
        # the run directory
        assert os.path.isfile(engine.get_run_file(run_id))
        assert os.path.isdir(engine.get_run_dir(run_id))
        # There is exactly one result file
        assert len(state.files) == 1
        assert 'results/greetings.txt' in state.files
        greetings = list()
        with open(state.files['results/greetings.txt'].filename, 'r') as f:
            for line in f:
                greetings.append(line.strip())
        assert len(greetings) == 2
        assert greetings[0] == 'Hello Alice!'
        assert greetings[1] == 'Hello Bob!'
        # Read workglow state should give the same result
        state = engine.get_run_state(run_id)
        assert state.is_success()
        assert len(state.files) == 1
        assert 'results/greetings.txt' in state.files
        greetings = list()
        with open(state.files['results/greetings.txt'].filename, 'r') as f:
            for line in f:
                greetings.append(line.strip())
        assert len(greetings) == 2
        assert greetings[0] == 'Hello Alice!'
        assert greetings[1] == 'Hello Bob!'
        # Re-run workflow will raise an error
        with pytest.raises(err.DuplicateRunError):
            engine.exec_workflow(
                run_id=run_id,
                template=template,
                source_dir=TEMPLATE_DIR,
                arguments=arguments
            )
        os.remove(engine.get_run_file(run_id))
        assert not os.path.isfile(engine.get_run_file(run_id))
        with pytest.raises(err.DuplicateRunError):
            engine.exec_workflow(
                run_id=run_id,
                template=template,
                source_dir=TEMPLATE_DIR,
                arguments=arguments
            )
        # After removing the run we can execute it again with the same identifier
        engine.remove_run(run_id)
        assert not os.path.isfile(engine.get_run_file(run_id))
        assert not os.path.isdir(engine.get_run_dir(run_id))
        state = engine.exec_workflow(
            run_id=run_id,
            template=template,
            source_dir=TEMPLATE_DIR,
            arguments=arguments
        )
        # Check for success and existince of file and folder
        assert state.is_success()
        assert os.path.isfile(engine.get_run_file(run_id))
        assert os.path.isdir(engine.get_run_dir(run_id))
        # Remove again to ensure that all files are removed correctly
        engine.remove_run(run_id)
        assert not os.path.isfile(engine.get_run_file(run_id))
        assert not os.path.isdir(engine.get_run_dir(run_id))
        # Error when trying to get state of unknown run
        with pytest.raises(err.UnknownRunError):
            engine.get_run_state(run_id)

    def test_run_with_invalid_cmd(self, tmpdir):
        """Execute the helloworld example with an invalid shell command."""
        # Read the workflow template
        doc = util.read_object(filename=TEMPLATE_WITH_INVALID_CMD)
        template = WorkflowTemplate.from_dict(doc)
        # Set the template argument values
        arguments = {
            'names': TemplateArgument(
                parameter=template.get_parameter('names'),
                value=FileHandle(NAMES_FILE)
            ),
            'sleeptime': TemplateArgument(
                parameter=template.get_parameter('sleeptime'),
                value=3
            )
        }
        # Run workflow syncronously
        engine = SyncWorkflowEngine(str(tmpdir))
        run_id = util.get_short_identifier()
        state = engine.exec_workflow(
            run_id=run_id,
            template=template,
            source_dir=TEMPLATE_DIR,
            arguments=arguments
        )
        assert state.is_error()
        assert len(state.messages) > 0
        print(state.messages)
        state = engine.get_run_state(run_id)
        assert state.is_error()
        assert len(state.messages) > 0

    def test_run_with_missing_file(self, tmpdir):
        """Execute the helloworld example with a reference to a missing file."""
        # Read the workflow template
        doc = util.read_object(filename=TEMPLATE_WITH_MISSING_FILE)
        template = WorkflowTemplate.from_dict(doc)
        # Set the template argument values
        arguments = {
            'names': TemplateArgument(
                parameter=template.get_parameter('names'),
                value=FileHandle(NAMES_FILE)
            ),
            'sleeptime': TemplateArgument(
                parameter=template.get_parameter('sleeptime'),
                value=3
            )
        }
        # Run workflow syncronously
        engine = SyncWorkflowEngine(str(tmpdir))
        run_id = util.get_short_identifier()
        state = engine.exec_workflow(
            run_id=run_id,
            template=template,
            source_dir=TEMPLATE_DIR,
            arguments=arguments
        )
        assert state.is_error()
        assert len(state.messages) > 0
        state = engine.get_run_state(run_id)
        assert state.is_error()
        assert len(state.messages) > 0
        print(state.messages)
        # An error is raised if the input file does not exist
        with pytest.raises(IOError):
            engine.exec_workflow(
                run_id=util.get_unique_identifier(),
                template=template,
                source_dir=TEMPLATE_DIR,
                arguments={
                    'names': TemplateArgument(
                        parameter=template.get_parameter('names'),
                        value=FileHandle(UNKNOWN_FILE)
                    ),
                    'sleeptime': TemplateArgument(
                        parameter=template.get_parameter('sleeptime'),
                        value=3
                    )
                }
            )
