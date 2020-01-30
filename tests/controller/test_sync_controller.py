# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the synchronous mode of the serial workflow controller."""

import os
import pytest

from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.core.files import FileHandle
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.parameter.value import TemplateArgument
from flowserv.model.run.base import RunHandle

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.workflow.state as st


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/template')
# Workflow templates
TEMPLATE_HELLOWORLD = os.path.join(TEMPLATE_DIR, './hello-world.yaml')
INVALID_TEMPLATE = './template-invalid-cmd.yaml'
TEMPLATE_WITH_INVALID_CMD = os.path.join(TEMPLATE_DIR, INVALID_TEMPLATE)
MISSING_FILE_TEMPLATE = './template-missing-file.yaml'
TEMPLATE_WITH_MISSING_FILE = os.path.join(TEMPLATE_DIR, MISSING_FILE_TEMPLATE)
# Input files
NAMES_FILE = os.path.join(TEMPLATE_DIR, './inputs/short-names.txt')
UNKNOWN_FILE = os.path.join(TEMPLATE_DIR, './tmp/no/file/here')


def test_run_helloworld(tmpdir):
    """Execute the helloworld example."""
    # Read the workflow template
    doc = util.read_object(filename=TEMPLATE_HELLOWORLD)
    template = WorkflowTemplate.from_dict(doc, sourcedir=TEMPLATE_DIR)
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
    engine = SerialWorkflowEngine(is_async=False)
    run_id = util.get_short_identifier()
    rundir = os.path.join(str(tmpdir), run_id)
    run = RunHandle(
        identifier=run_id,
        workflow_id='0001',
        group_id='0001',
        state=st.StatePending(),
        arguments=dict(),
        rundir=rundir
    )
    state = engine.exec_workflow(
        run=run,
        template=template,
        arguments=arguments
    )
    # For completeness. Cancel run should have no effect
    engine.cancel_run(run_id)
    # Expect the result to be success
    assert state.is_success()
    # The base directory for the run will have been created by the engine
    assert os.path.isdir(run.rundir)
    # There is exactly one result file
    assert len(state.resources) == 1
    greetings_file = state.resources.get_resource(name='results/greetings.txt')
    greetings = list()
    with open(greetings_file.filename, 'r') as f:
        for line in f:
            greetings.append(line.strip())
    assert len(greetings) == 2
    assert greetings[0] == 'Hello Alice!'
    assert greetings[1] == 'Hello Bob!'


def test_run_with_invalid_cmd(tmpdir):
    """Execute the helloworld example with an invalid shell command."""
    # Read the workflow template
    doc = util.read_object(filename=TEMPLATE_WITH_INVALID_CMD)
    template = WorkflowTemplate.from_dict(doc, sourcedir=TEMPLATE_DIR)
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
    engine = SerialWorkflowEngine(is_async=True)
    run_id = util.get_short_identifier()
    rundir = os.path.join(str(tmpdir), run_id)
    run = RunHandle(
        identifier=run_id,
        workflow_id='0001',
        group_id='0001',
        state=st.StatePending(),
        arguments=dict(),
        rundir=rundir
    )
    state = engine.exec_workflow(
        run=run,
        template=template,
        arguments=arguments,
        run_async=False
    )
    assert state.is_error()
    assert len(state.messages) > 0


def test_run_with_missing_file(tmpdir):
    """Execute the helloworld example with a reference to a missing file.
    """
    # Read the workflow template
    doc = util.read_object(filename=TEMPLATE_WITH_MISSING_FILE)
    template = WorkflowTemplate.from_dict(doc, sourcedir=TEMPLATE_DIR)
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
    engine = SerialWorkflowEngine()
    run_id = util.get_short_identifier()
    rundir = os.path.join(str(tmpdir), run_id)
    run = RunHandle(
        identifier=run_id,
        workflow_id='0001',
        group_id='0001',
        state=st.StatePending(),
        arguments=dict(),
        rundir=rundir
    )
    state = engine.exec_workflow(
        run=run,
        template=template,
        arguments=arguments,
        run_async=False
    )
    assert state.is_error()
    assert len(state.messages) > 0
    # An error is raised if the input file does not exist
    with pytest.raises(err.UnknownFileError):
        engine.exec_workflow(
            run=run,
            template=template,
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
