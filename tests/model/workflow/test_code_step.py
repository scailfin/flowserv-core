# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the code workflow step that evaluates a given function on the
current workflow state.
"""

from flowserv.model.workflow.serial import CodeStep


def my_add(x: int, y: int) -> int:
    """Simple workflow step function."""
    z = x + y
    return z


def test_exec_code_step():
    """Test executing a Python function as a step in a sreial workflow."""
    args = {'x': 1, 'y': 2}
    step = CodeStep(func=my_add, output='z')
    step.exec(arguments=args)
    assert args == {'x': 1, 'y': 2, 'z': 3}
    # Test renaming arguments.
    step = CodeStep(func=my_add, varnames={'x': 'z'}, output='x')
    step.exec(arguments=args)
    assert args == {'x': 5, 'y': 2, 'z': 3}
    # Execute function but ignore output.
    step = CodeStep(func=my_add)
    step.exec(arguments=args)
    assert args == {'x': 5, 'y': 2, 'z': 3}
