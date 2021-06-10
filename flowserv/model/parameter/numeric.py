# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for numeric parameter values. Numeric parameters can specify
ranges of valid values or minimum and maximum values.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from flowserv.model.parameter.base import Parameter, PARA_FLOAT, PARA_INT

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Identifier for numeric parameter types."""
NUMERIC_TYPES = [PARA_FLOAT, PARA_INT]


# -- Range constraints --------------------------------------------------------

class RangeConstraint(object):
    """Range constraint for numeric parameter values. Range constraints are
    specified as strings following standard interval notation where square
    brackets denote closed intervals and round brackets denote open intervals.
    Infinity and negative infinity are represented as 'inf' and '-inf',
    respectively. They may also be represented as an empty string.

    Valid range interval strings are: [x,y], (x,y), [x,y), and (x,y] where x
    may either be a number, '', or '-inf', and y may either be a number, '',
    or 'inf'.
    """
    def __init__(self, left_boundary: Dict, right_boundary: Dict):
        """Initialize the interval boundaries.

        Parameters
        ----------
        left_boundary: dict
            Left boundary of the interval.
        right_boundary: dict
            Right boundary of the interval.
        """
        if left_boundary['value'] > right_boundary['value']:
            raise ValueError('invalid interval boundaries')
        self.left_boundary = left_boundary
        self.right_boundary = right_boundary

    @classmethod
    def from_string(cls, value: str):
        """Create range constraint instance from string representation.

        Parameters
        ----------
        value: string
            String representation for a range constraint.

        Returns
        -------
        flowserv.model.parameter.numeric.RangeConstraint

        Raises
        ------
        ValueError
        """
        # Expects a string with exactly one comma.
        tokens = value.split(',')
        if len(tokens) != 2:
            raise ValueError("invalid interval '{}'".format(value))
        leftside, rightside = tokens[0].strip(), tokens[1].strip()
        # The left side of the interval will either start with an open square
        # or open bracket.
        if leftside.startswith('['):
            is_open = False
        elif leftside.startswith('('):
            is_open = True
        else:
            raise ValueError("invalid interval '{}'".format(value))
        lval = leftside[1:] if leftside[1:] else '-inf'
        left_boundary = {'value': float(lval), 'open': is_open}
        # The right side of the interval will either end with a closed square
        # or closed bracket.
        if rightside.endswith(']'):
            is_open = False
        elif rightside.endswith(')'):
            is_open = True
        else:
            raise ValueError("invalid interval '{}'".format(value))
        rval = rightside[:-1] if rightside[:-1] else 'inf'
        right_boundary = {'value': float(rval), 'open': is_open}
        return cls(left_boundary, right_boundary)

    def is_closed(self) -> bool:
        """Returns True if both interval boundaries are closed.

        Returns
        -------
        bool
        """
        lopen = self.left_boundary['open']
        ropen = self.right_boundary['open']
        return not lopen and not ropen

    def max_value(self) -> Union[int, float]:
        """Get the right boundary value for the interval.

        Returns
        -------
        int or float
        """
        return self.right_boundary['value']

    def min_value(self) -> Union[int, float]:
        """Get the left boundary value for the interval.

        Returns
        -------
        int or float
        """
        return self.left_boundary['value']

    def to_string(self) -> str:
        """Get string representation for the range constraint.

        Returns
        -------
        string
        """
        template = '(' if self.left_boundary['open'] else '['
        template += '{},{}'
        template += ')' if self.right_boundary['open'] else ']'
        return template.format(
            self.left_boundary['value'],
            self.right_boundary['value']
        )

    def validate(self, value: Union[int, float]) -> bool:
        """Validate that the given value is within the interval. Raises an
        error if the value is not within the interval.

        Parameters
        ----------
        value: int or float
            Value that is being tested.

        Raises
        ------
        flowserv.error.InvalidArgumentError
        """
        if self.left_boundary['open']:
            if self.left_boundary['value'] >= value:
                return False
        else:
            if self.left_boundary['value'] > value:
                return False
        if self.right_boundary['open']:
            if value >= self.right_boundary['value']:
                return False
        else:
            if value > self.right_boundary['value']:
                return False
        return True


@dataclass
class Boundary(object):
    """Boundary definition for a range interval. Contains the interval boundary
    value and a flag defining whether the interval is open (i.e., excludes the
    defined value) or closed (i.e., includes the defined value).
    """
    # Interval boundary value.
    value: Union[int, float, str] = None
    # Flag indicating whether the boundary is open or closed.
    is_closed: Optional[bool] = True

    def to_left_boundary(self) -> str:
        """Return a left-boundary representation.

        Returns
        -------
        string
        """
        return '{}{}'.format('[' if self.is_closed else '(', self.value)

    def to_right_boundary(self) -> str:
        """Return a left-boundary representation.

        Returns
        -------
        string
        """
        return '{}{}'.format(self.value, ']' if self.is_closed else ')')


IntervalBoundary = Union[int, float, str, Boundary]


def range_constraint(
    left: Optional[IntervalBoundary] = None,
    right: Optional[IntervalBoundary] = None
) -> RangeConstraint:
    """Create a range constraint instance from a given pair of interval
    boundaries. Returns None if no boundary is given.

    Parameters
    ----------
    left: flowserv.model.parameter.numeric.IntervalBoundary, default=None
        Left boundary for range constraint.
    right: flowserv.model.parameter.numeric.IntervalBoundary, default=None
        Right boundary for range constraint.

    Returns
    -------
    flowserv.model.parameter.numeric.RangeConstraint
    """
    # Returns None if neither interval boundary is given.
    if left is None and right is None:
        return None
    if left is None:
        interval = '[,'
    elif isinstance(left, Boundary):
        interval = '{},'.format(left.to_left_boundary())
    else:
        interval = '[{},'.format(left)
    if right is None:
        interval += ']'
    elif isinstance(right, Boundary):
        interval += right.to_right_boundary()
    else:
        interval += '{}]'.format(right)
    return RangeConstraint.from_string(interval)


# -- Numeric parameter declarations -------------------------------------------

class Numeric(Parameter):
    """Base class for numeric parameter types. Extends the base class with an
    optional range constraint.
    """
    def __init__(
        self, dtype: str, name: str, index: Optional[int] = 0,
        label: Optional[str] = None, help: Optional[str] = None,
        default: Optional[Union[int, float]] = None, required: Optional[bool] = False,
        group: Optional[str] = None, constraint: Optional[RangeConstraint] = None
    ):
        """Initialize the base properties for a numeric parameter declaration.

        Parameters
        ----------
        dtype: string
            Parameter type identifier.
        name: string
            Unique parameter identifier
        index: int
            Index position of the parameter (for display purposes).
        label: string
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: int or float, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        group: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        constraint: flowserv.model.parameter.numeric.RangeConstraint
            Optional range constraint defining valid parameter values.
        """
        if dtype not in NUMERIC_TYPES:
            raise ValueError("invalid numeric type '{}'".format(dtype))
        super(Numeric, self).__init__(
            dtype=dtype,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group
        )
        self.constraint = constraint

    def cast(self, value: Any) -> Any:
        """Convert the given value into a numeric value. Raises an error if the
        value cannot be converted to the respective numeric type of if it does
        not satisfy the optional range constraint.

        Parameters
        ----------
        value: any
            User-provided value for a template parameter.

        Returns
        -------
        sting, float, or int

        Raises
        ------
        flowserv.error.InvalidArgumentError
        """
        if value in ['-inf', 'inf']:
            value = float(value)
        elif value in [float('-inf'), float('inf')]:
            value = float(value)
        elif self.dtype == PARA_INT:
            try:
                value = int(value)
            except (TypeError, ValueError):
                raise err.InvalidArgumentError("no int '{}'".format(value))
        else:
            try:
                value = float(value)
            except (TypeError, ValueError):
                raise err.InvalidArgumentError("no float '{}'".format(value))
        if self.constraint is not None:
            if not self.constraint.validate(value):
                msg = '{} not in {}'.format(value, self.constraint.to_string())
                raise err.InvalidArgumentError(msg)
        return value

    @staticmethod
    def from_dict(doc: Dict, validate: Optional[bool] = True) -> Numeric:
        """Get numeric parameter instance from a dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for numeric parameter.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.numeric.Numeric

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            try:
                util.validate_doc(
                    doc,
                    mandatory=pd.MANDATORY,
                    optional=pd.OPTIONAL + ['range']
                )
                constraint = None
                if 'range' in doc:
                    constraint = RangeConstraint.from_string(doc['range'])
            except (ValueError, TypeError) as ex:
                raise err.InvalidParameterError(str(ex))
        try:
            constraint = None
            if 'range' in doc:
                constraint = RangeConstraint.from_string(doc['range'])
        except (ValueError, TypeError) as ex:
            raise err.InvalidParameterError(str(ex))
        return Numeric(
            dtype=doc[pd.TYPE],
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            label=doc.get(pd.LABEL),
            help=doc.get(pd.HELP),
            default=doc.get(pd.DEFAULT),
            required=doc[pd.REQUIRED],
            group=doc.get(pd.GROUP),
            constraint=constraint
        )

    def to_dict(self) -> Dict:
        """Get dictionary serialization for the parameter declaration. Adds
        a serialization for an optional range constraint to the serialization
        of the base class.

        Returns
        -------
        dict
        """
        obj = super().to_dict()
        if self.constraint is not None:
            obj['range'] = self.constraint.to_string()
        return obj


class Int(Numeric):
    """Base class for integer parameter types."""
    def __init__(
        self, name: str, index: Optional[int] = 0, label: Optional[str] = None,
        help: Optional[str] = None, default: Optional[int] = None,
        required: Optional[bool] = False, group: Optional[str] = None,
        min: Optional[IntervalBoundary] = None, max: Optional[IntervalBoundary] = None
    ):
        """Initialize the base properties for a integer parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        index: int
            Index position of the parameter (for display purposes).
        label: string
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: int, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        group: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        min: flowserv.model.parameter.numeric.IntervalBoundary, default=None
            Optional range constraint minimum value definition.
        max: flowserv.model.parameter.numeric.IntervalBoundary, default=None
            Optional range constraint maximum value definition.
        """
        super(Int, self).__init__(
            dtype=PARA_INT,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group,
            constraint=range_constraint(min, max)
        )


class Float(Numeric):
    """Base class for float parameter types."""
    def __init__(
        self, name: str, index: Optional[int] = 0, label: Optional[str] = None,
        help: Optional[str] = None, default: Optional[float] = None,
        required: Optional[bool] = False, group: Optional[str] = None,
        min: Optional[IntervalBoundary] = None, max: Optional[IntervalBoundary] = None
    ):
        """Initialize the base properties for a float parameter declaration.

        Parameters
        ----------
        name: string
            Unique parameter identifier
        index: int
            Index position of the parameter (for display purposes).
        label: string
            Human-readable parameter name.
        help: string, default=None
            Descriptive text for the parameter.
        default: float, default=None
            Optional default value.
        required: bool, default=False
            Is required flag.
        group: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        min: flowserv.model.parameter.numeric.IntervalBoundary, default=None
            Optional range constraint minimum value definition.
        max: flowserv.model.parameter.numeric.IntervalBoundary, default=None
            Optional range constraint maximum value definition.
        """
        super(Float, self).__init__(
            dtype=PARA_FLOAT,
            name=name,
            index=index,
            label=label,
            help=help,
            default=default,
            required=required,
            group=group,
            constraint=range_constraint(min, max)
        )
