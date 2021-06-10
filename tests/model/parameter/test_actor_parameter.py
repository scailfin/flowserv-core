# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for workflow actor parameter declarations."""

import pytest

from flowserv.model.parameter.actor import Actor, ActorValue, PARA_ACTOR

import flowserv.error as err


def test_actor_parameter_from_dict():
    """Test generating a string parameter declaration from a dictionary
    serialization.
    """
    para = Actor.from_dict(
        Actor.to_dict(
            Actor.from_dict({
                'name': '0000',
                'dtype': PARA_ACTOR,
                'label': 'Step 1',
                'index': 1,
                'help': 'Your first step',
                'defaultValue': {'image': 'test', 'commands': ['step1']},
                'isRequired': True,
                'group': 'workflow'
            })
        )
    )
    assert para.is_actor()
    assert para.name == '0000'
    assert para.dtype == PARA_ACTOR
    assert para.label == 'Step 1'
    assert para.index == 1
    assert para.help == 'Your first step'
    assert para.default == {'image': 'test', 'commands': ['step1']}
    assert para.required
    assert para.group == 'workflow'


def test_actor_step_factory():
    """Test getting workflow step instances for an actor parameter."""
    para = Actor('step')
    doc = {'image': 'test', 'commands': ['step1'], 'env': {'a': 'x'}}
    step = para.cast(ActorValue(spec=doc))
    assert step.spec == doc
    # Error cases.
    with pytest.raises(err.InvalidArgumentError):
        para.cast('unknown')


def test_invalid_serialization():
    """Test errors for invalid serializations."""
    with pytest.raises(err.InvalidParameterError):
        Actor.from_dict({
            'name': '0000',
            'dtype': PARA_ACTOR,
        })
    Actor.from_dict({
        'name': '0000',
        'dtype': 'unknown',
        'index': 0,
        'label': 'Name',
        'isRequired': True
    }, validate=False)
    with pytest.raises(ValueError):
        Actor.from_dict({
            'name': '0000',
            'dtype': 'unknown',
            'index': 0,
            'label': 'Name',
            'isRequired': True
        }, validate=True)
