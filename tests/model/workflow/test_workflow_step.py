# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the different step types for a serial workflow."""

import pytest

from flowserv.model.workflow.step import CodeStep, ContainerStep, WorkflowStep


def my_add(x: int, y: int) -> int:
    """Simple workflow step function."""
    z = x + y
    return z


def test_container_step():
    """Test add method of the container step."""
    step = ContainerStep(identifier='test', image='test').add('A').add('B')
    assert step.image == 'test'
    assert step.commands == ['A', 'B']


def test_exec_func_step():
    """Test executing a Python function as a step in a serial workflow."""
    args = {'x': 1, 'y': 2}
    step = CodeStep(identifier='test', func=my_add, arg='z')
    step.exec(context=args)
    assert args == {'x': 1, 'y': 2, 'z': 3}
    # Test renaming arguments.
    step = CodeStep(identifier='test', func=my_add, varnames={'x': 'z'}, arg='x')
    step.exec(context=args)
    assert args == {'x': 5, 'y': 2, 'z': 3}
    # Execute function but ignore output.
    step = CodeStep(identifier='test', func=my_add)
    step.exec(context=args)
    assert args == {'x': 5, 'y': 2, 'z': 3}


def test_step_type():
    """Test methods that distinguish different step types."""
    # CodeStep
    step = CodeStep(
        identifier='test',
        func=my_add,
        arg='z',
        inputs=['a', 'b'],
        outputs=['x', 'y']
    )
    assert step.identifier == 'test'
    assert step.arg == 'z'
    assert step.inputs == ['a', 'b']
    assert step.outputs == ['x', 'y']
    assert step.is_code_step()
    assert not step.is_container_step()
    # ContainerStep
    step = ContainerStep(
        identifier='test',
        image='test',
        inputs=['a', 'b'],
        outputs=['x', 'y']
    )
    assert step.identifier == 'test'
    assert step.image == 'test'
    assert step.inputs == ['a', 'b']
    assert step.outputs == ['x', 'y']
    assert step.is_container_step()
    assert not step.is_code_step()
    # Empty inputs.
    step = CodeStep(identifier='test', func=my_add)
    assert step.inputs == []
    assert step.outputs == []
    step = ContainerStep(identifier='test', image='test')
    assert step.inputs == []
    assert step.outputs == []
    # Invalid step type.
    with pytest.raises(ValueError):
        WorkflowStep(identifier='test', step_type=-1)
