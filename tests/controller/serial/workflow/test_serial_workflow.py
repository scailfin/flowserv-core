# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for methods that initialize serial workflows."""

import pytest

from flowserv.model.workflow.step import ContainerStep
from flowserv.controller.serial.workflow.base import SerialWorkflow
from flowserv.model.parameter.string import String

import flowserv.error as err


def test_initialize_workflow():
    """Test initializing the serial workflow with different argument combinations."""
    # -- Initialize without arguments -----------------------------------------
    wf = SerialWorkflow()
    assert len(wf.steps) == 0
    assert len(wf.parameters) == 0
    assert wf.workers is not None
    # -- Set workflow steps at initialization ---------------------------------
    wf = SerialWorkflow(
        steps=[
            ContainerStep(identifier='s1', image='test_1'),
            ContainerStep(identifier='s2', image='test_2')
        ]
    )
    assert len(wf.steps) == 2
    assert [s.image for s in wf] == ['test_1', 'test_2']
    assert wf.steps[0].image == 'test_1'
    assert wf.steps[1].image == 'test_2'
    assert len(wf.parameters) == 0
    assert wf.workers is not None
    # -- Set template parameters at initialization ----------------------------
    wf = SerialWorkflow(parameters=[String('a'), String('b')])
    assert len(wf.steps) == 0
    assert len(wf.parameters) == 2
    assert 'a' in wf.parameters
    assert 'b' in wf.parameters
    assert wf.workers is not None
    # -- Error when initializing parameters with duplicate names --------------
    with pytest.raises(err.InvalidTemplateError):
        SerialWorkflow(parameters=[String('a'), String('a')])


def test_add_workflow_parameter():
    """Test adding template parameters to a serial workflow."""
    wf = SerialWorkflow()
    wf = wf.add_parameter(String('a'))
    assert len(wf.parameters) == 1
    wf = wf.add_parameter(String('b')).add_parameter(String('a'))
    assert len(wf.parameters) == 2


def test_add_workflow_step():
    """Test adding steps to a serial workflow."""
    wf = SerialWorkflow()
    # -- Add container step ---------------------------------------------------
    wf = wf.add_container_step(
        identifier='test',
        image='testimg',
        commands=['a', 'b'],
        env={'x': 1},
        inputs=['a', 'b'],
        outputs=['x', 'y']
    )
    assert len(wf.steps) == 1
    s = wf.steps[0]
    assert s.is_container_step()
    assert s.identifier == 'test'
    assert s.image == 'testimg'
    assert s.commands == ['a', 'b']
    assert s.env == {'x': 1}
    assert s.inputs == ['a', 'b']
    assert s.outputs == ['x', 'y']
    # -- Add code step --------------------------------------------------------
    wf = wf.add_code_step(
        identifier='test',
        func=str.lower,
        arg='x',
        varnames={'x': 'y'},
        inputs=['a', 'b'],
        outputs=['x', 'y']
    )
    assert len(wf.steps) == 2
    s = wf.steps[1]
    assert s.is_code_step()
    assert s.func('ABC') == 'abc'
    assert s.identifier == 'test'
    assert s.arg == 'x'
    assert s.varnames == {'x': 'y'}
    assert s.inputs == ['a', 'b']
    assert s.outputs == ['x', 'y']
