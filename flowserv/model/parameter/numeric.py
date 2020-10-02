# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Declarations for numeric parameter values. Numeric parameters can specify
ranges of valid values or minimum and maximum values.
"""

from flowserv.model.parameter.base import ParameterBase

import flowserv.error as err
import flowserv.model.parameter.base as pd
import flowserv.util as util


"""Identifier for numeric parameter types."""
PARA_FLOAT = 'float'
PARA_INT = 'int'
NUMERIC_TYPES = [PARA_FLOAT, PARA_INT]


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
    def __init__(self, left_boundary, right_boundary):
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
    def from_string(cls, value):
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

    def is_closed(self):
        """Returns True if both interval boundaries are closed.

        Returns
        -------
        bool
        """
        lopen = self.left_boundary['open']
        ropen = self.right_boundary['open']
        return not lopen and not ropen

    def max_value(self):
        """Get the right boundary value for the interval.

        Returns
        -------
        int or float
        """
        return self.right_boundary['value']

    def min_value(self):
        """Get the left boundary value for the interval.

        Returns
        -------
        int or float
        """
        return self.left_boundary['value']

    def to_string(self):
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

    def validate(self, value):
        """Validate that the given value is within the interval. Raises an
        error if the value is not within the interval.

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


class NumericParameter(ParameterBase):
    """Base class for numeric parameter types. Extends the base class with an
    optional range constraint.
    """
    def __init__(
        self, para_id, type_id, name, index, description=None,
        default_value=None, is_required=False, module_id=None,
        constraint=None
    ):
        """Initialize the base properties for a numeric parameter declaration.

        Parameters
        ----------
        para_id: string
            Unique parameter identifier
        type_id: string
            Parameter type identifier.
        name: string
            Human-readable parameter name.
        index: int
            Index position of the parameter (for display purposes).
        description: string, default=None
            Descriptive text for the parameter.
        default_value: any, default=None
            Optional default value.
        is_required: bool, default=False
            Is required flag.
        module_id: string, default=None
            Optional identifier for parameter group that this parameter
            belongs to.
        constraint: flowserv.model.parameter.numeric.RangeConstraint
            Optional range constraint defining valid parameter values.
        """
        if type_id not in NUMERIC_TYPES:
            raise ValueError("invalid numeric type '{}'".format(type_id))
        super(NumericParameter, self).__init__(
            para_id=para_id,
            type_id=type_id,
            name=name,
            index=index,
            description=description,
            default_value=default_value,
            is_required=is_required,
            module_id=module_id
        )
        self.constraint = constraint

    @classmethod
    def from_dict(cls, doc, validate=True):
        """Get numeric parameter instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for numeric parameter.
        validate: bool, default=True
            Validate the serialized object if True.

        Returns
        -------
        flowserv.model.parameter.numeric.NumericParameter

        Raises
        ------
        flowserv.error.InvalidParameterError
        """
        if validate:
            try:
                util.validate_doc(
                    doc,
                    mandatory=[pd.ID, pd.TYPE, pd.NAME, pd.INDEX, pd.REQUIRED],
                    optional=[pd.DESC, pd.DEFAULT, pd.MODULE, 'range']
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
        return cls(
            para_id=doc[pd.ID],
            type_id=doc[pd.TYPE],
            name=doc[pd.NAME],
            index=doc[pd.INDEX],
            description=doc.get(pd.DESC),
            default_value=doc.get(pd.DEFAULT),
            is_required=doc[pd.REQUIRED],
            module_id=doc.get(pd.MODULE),
            constraint=constraint
        )

    def to_argument(self, value):
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
        elif self.type_id == PARA_INT:
            try:
                value = int(value)
            except OverflowError:
                value = float('inf')
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

    def to_dict(self):
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


# -- Helper Methods -----------------------------------------------------------

def is_float(para: ParameterBase) -> bool:
    """Test if the given parameter is of type PARA_FLOAT.

    Parameters
    ----------
    para: flowserv.model.parameter.base.ParameterBase
        Template parameter definition.

    Returns
    -------
    bool
    """
    return para.type_id == PARA_FLOAT


def is_int(para: ParameterBase) -> bool:
    """Test if the given parameter is of type PARA_INT.

    Parameters
    ----------
    para: flowserv.model.parameter.base.ParameterBase
        Template parameter definition.

    Returns
    -------
    bool
    """
    return para.type_id == PARA_INT


def is_numeric(para: ParameterBase) -> bool:
    """Test if the given parameter is of type PARA_FLOAT or PARA_INT.

    Parameters
    ----------
    para: flowserv.model.parameter.base.ParameterBase
        Template parameter definition.

    Returns
    -------
    bool
    """
    return para.type_id in NUMERIC_TYPES
