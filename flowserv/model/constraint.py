# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of methods to check and enforce constraints that are currently
defined for the objects that are stored in the database.
"""

import re

import flowserv.error as err


def validate_identifier(identifier: str) -> bool:
    """Validate the given identifier to ensure that: (i) it's length is between
    1 and 32, and (ii) it only contains letters (A-Z), digites, or underscore.

    If the idnentifier is None it is considered valid. If an invalid identifier
    is given a ValueError will be raised.

    Returns True if the identifier is valid.

    Parameters
    ----------
    identifier: string
        Unique identifier string or None

    Raises
    ------
    ValueError
    """
    if identifier is None:
        return True
    errmsg = "invalid workflow identifier '{}'"
    if not 1 <= len(identifier) <= 32:
        raise ValueError(errmsg.format(identifier))
    if not re.match('^[a-zA-Z0-9_]+$', identifier):
        raise ValueError(errmsg.format(identifier))
    return True


def validate_name(name):
    """Validate the given name. Raises an error if the given name violates the
    current constraints for names. The constraints are:

    - no empty or missing names
    - names can be at most 512 characters long

    Parameters
    ----------
    name: string
        Name that is being validated

    Raises
    ------
    flowserv.error.ConstraintViolationError
    """
    if name is None:
        raise err.ConstraintViolationError('missing name')
    name = name.strip()
    if name == '' or len(name) > 512:
        raise err.ConstraintViolationError('invalid name')
