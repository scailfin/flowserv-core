# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of methods to check and enforce constraints that are currently
defined for the objects that are stored in the database.
"""

import flowserv.core.error as err


def validate_name(name, con=None, sql=None, args=None):
    """Validate the given name. Raises an error if the given name violates the
    current constraints for names. The constraints are:

    - no empty or missing names
    - names can be at most 512 characters long
    - names are unique (if sql statement is given)

    To test name uniqueness a database connection and SQL statement is expected.
    The SQL statement should be parameterized with the name as the only
    parameter. If no query arguments are given the name is expected to be the
    only argument.

    Parameters
    ----------
    name: string
        Name that is being validated
    con: DB-API 2.0 database connection, optional
        Connection to underlying database
    sql: string, optional
        SQL query to check if a given name exists or not
    args: tuple or list, optional
        Optional list of arguments for the query.

    Raises
    ------
    flowserv.core.error.ConstraintViolationError
    """
    if name is None:
        raise err.ConstraintViolationError('missing name')
    name = name.strip()
    if name == '' or len(name) > 512:
        raise err.ConstraintViolationError('invalid name')
    # Validate uniqueness if a database connection and SQL statement are given
    if con is None or sql is None:
        return
    if args is not None:
        query_args = args
    else:
        query_args = (name,)
    if not con.execute(sql, query_args).fetchone() is None:
        raise err.ConstraintViolationError("name '{}' exists".format(name))
