# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test to verfiy that the constraint checker works properly."""

import pytest

import flowserv.core.error as err
import flowserv.model.constraint as constraint
import flowserv.tests.db as db
import flowserv.core.util as util


def test_validate_name(tmpdir):
    """Test the object name constraint."""
    # Create new database and insert one new user
    con = db.init_db(str(tmpdir)).connect()
    user_id = util.get_unique_identifier()
    sql = 'INSERT INTO api_user(user_id, secret, name, active) '
    sql += 'VALUES(?, ?, ?, 1)'
    con.execute(sql, (user_id, user_id, user_id))
    con.commit()
    # The automatically generated user identifier is a valid name
    constraint.validate_name(name=user_id)
    # Test error conditions
    # - Missing values
    with pytest.raises(err.ConstraintViolationError) as ex:
        constraint.validate_name(name=None)
        assert str(ex) == 'missing name'
    # - Invalid name
    with pytest.raises(err.ConstraintViolationError) as ex:
        constraint.validate_name(name=' ')
        assert str(ex) == 'invalid name'
    with pytest.raises(err.ConstraintViolationError) as ex:
        constraint.validate_name(name='A' * 1024)
        assert str(ex) == 'invalid name'
    # - Duplicate name (also test option to exclude a given row)
    sql = 'SELECT name FROM api_user WHERE name = ?'
    with pytest.raises(err.ConstraintViolationError) as ex:
        constraint.validate_name(name=user_id, con=con, sql=sql)
        assert str(ex) == 'name \'{}\' exists'.format(user_id)
    sql = 'SELECT name FROM api_user WHERE name = ? AND user_id <> ?'
    args = (user_id, user_id)
    constraint.validate_name(name=user_id, con=con, sql=sql, args=args)
