# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of methods to check and enforce constraints that are currently
defined for the objects that are stored in the database.
"""

import flowserv.error as err


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
