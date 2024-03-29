# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for numeric parameter declarations."""

import pytest

from flowserv.model.parameter.numeric import Boundary, RangeConstraint, range_constraint
from flowserv.model.parameter.numeric import Int, Float, Numeric
from flowserv.model.parameter.numeric import PARA_INT, PARA_FLOAT

import flowserv.error as err


# -- Range constraints --------------------------------------------------------

@pytest.mark.parametrize(
    'text,assert_neginf,assert_5,assert_inf',
    [
        ('[,5)', True, False, False),
        ('(,5)', False, False, False),
        ('[,5]', True, True, False),
        ('(,5]', False, True, False),
        ('[5,)', False, True, False),
        ('(5,)', False, False, False),
        ('[5,]', False, True, True),
        ('(5,]', False, False, True)
    ]
)
def test_inf_range_constraint_intervals(
    text, assert_neginf, assert_5, assert_inf
):
    """Test different interval boundaries for range constraints that include
    infinity.
    """
    constraint = RangeConstraint.from_string(
        RangeConstraint.from_string(text).to_string()
    )
    assert constraint.validate(float('-inf')) == assert_neginf
    assert constraint.validate(5) == assert_5
    assert constraint.validate(float('inf')) == assert_inf


@pytest.mark.parametrize(
    'boundary,left,right',
    [(Boundary(1), '[1', '1]'), (Boundary(1, is_closed=False), '(1', '1)')]
)
def test_range_boundary(boundary, left, right):
    """Test range boundary data class."""
    assert boundary.to_left_boundary() == left
    assert boundary.to_right_boundary() == right


@pytest.mark.parametrize(
    'left,right,result',
    [
        (Boundary(1), Boundary(5), True),
        (1, 5, True),
        (Boundary(1), Boundary(5, is_closed=False), False),
        (Boundary(1), None, True),
        (None, Boundary(1), False)
    ]
)
def test_range_constraint_generator(left, right, result):
    """Test the range constraint generator function."""
    constraint = range_constraint(left=left, right=right)
    assert constraint.validate(5) == result
    # Ensure that the constraint is set correctly during parameter creation.
    para = Int('test', min=left, max=right)
    if result:
        para.cast(5)
    else:
        with pytest.raises(err.InvalidArgumentError):
            para.cast(5)


@pytest.mark.parametrize(
    'text, assert_5, assert_6, assert_7',
    [
        ('[ 5, 7 ]', True, True, True),
        ('( 5, 7 ]', False, True, True),
        ('[ 5, 7 )', True, True, False),
        ('( 5, 7 )', False, True, False)
    ]
)
def test_range_constraint_intervals(text, assert_5, assert_6, assert_7):
    """Test different interval boundaries for range constraints."""
    constraint = RangeConstraint.from_string(
        RangeConstraint.from_string(text).to_string()
    )
    assert constraint.validate(5) == assert_5
    assert constraint.validate(6) == assert_6
    assert constraint.validate(7) == assert_7


@pytest.mark.parametrize(
    'text',
    ['', '[,e,]', '4,8]', '[4,5', '[a,b]', '[5.6,2.7]']
)
def test_range_errors(text):
    """Test error swhen parsing invalid range intervals."""
    with pytest.raises(ValueError):
        RangeConstraint.from_string(text)


@pytest.mark.parametrize(
    'text,is_closed',
    [('[0,1]', True), ('(0,1]', False), ('[0,1)', False), ('(0,1)', False)]
)
def test_range_minmax_and_isclosed(text, is_closed):
    """Test min,max and is_closed methods of the range constraint."""
    constraint = RangeConstraint.from_string(text)
    assert constraint.min_value() == 0
    assert constraint.max_value() == 1
    assert constraint.is_closed() == is_closed


# -- Numeric parameters -------------------------------------------------------

def test_create_numeric_parameter_error():
    """Test error cases when creating numeric parameters."""
    # -- Invalid type identifier 'string' -------------------------------------
    with pytest.raises(KeyError):
        Float.from_dict({
            'name': '0000',
            'label': 'X',
            'isRequired': False
        }, validate=False)
    with pytest.raises(err.InvalidParameterError):
        Float.from_dict({
            'name': '0000',
            'label': 'X',
            'isRequired': False,
            'range': '0-1'
        }, validate=False)
    with pytest.raises(ValueError):
        Numeric.from_dict({
            'name': '0000',
            'dtype': 'string',
            'label': 'X',
            'index': 0,
            'isRequired': False
        })
    # -- Invalid document (missing name) --------------------------------------
    with pytest.raises(err.InvalidParameterError):
        Int.from_dict({
            'index': 0,
            'isRequired': False
        })


def test_numeric_parameter():
    """Test creating instances of numeric parameters."""
    para = Int('val')
    assert para.is_int()
    para = Float('val')
    assert para.is_float()


@pytest.mark.parametrize(
    'dtype,range',
    [
        (PARA_INT, None),
        (PARA_INT, '(5,]'),
        (PARA_FLOAT, None),
        (PARA_FLOAT, '(5,]')
    ]
)
def test_numeric_parameter_deserialization(dtype, range):
    """Test creating numeric parameters from dictinaries."""
    doc = {
        'name': '0000',
        'dtype': dtype,
        'label': 'X',
        'index': 0,
        'isRequired': False
    }
    if range is not None:
        doc['range'] = range
    para = Numeric.from_dict(Numeric.from_dict(doc).to_dict())
    if range is None:
        assert para.cast('5') == 5
    else:
        with pytest.raises(err.InvalidArgumentError):
            para.cast('5')
    assert para.cast('6') == 6
    assert para.cast(7) == 7
    assert para.is_numeric()
    assert para.cast('inf') == float('inf')
    with pytest.raises(err.InvalidArgumentError):
        para.cast('x')
