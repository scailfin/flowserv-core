# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of methods to check and enforce constraints that are currently
defined for the objects that are stored in the database.
"""

import robcore.error as err


def validate_name(name, con=None, sql=None):
    """Validate the given name. Raises an error if the given name violates the
    current constraints for names. The constraints are:

    - no empty or missing names
    - names can be at most 512 characters long
    - names are unique (if sql statement is given)

    To test name uniqueness a database connection and SQL statement is expected.
    The SQL statement should be parameterized with the name as the only
    parameter.

    Raises
    ------
    robcore.error.ConstraintViolationError
    """
    if name is None:
        raise err.ConstraintViolationError('missing name')
    name = name.strip()
    if name == '' or len(name) > 512:
        raise err.ConstraintViolationError('invalid name')
    # Validate uniqueness if a database connection and SQL statement are given
    if con is None or sql is None:
        return
    with con.cursor() as cur:
        if not con.execute(sql, (name,)).fetchone() is None:
            raise err.ConstraintViolationError('name \'{}\' exists'.format(name))
