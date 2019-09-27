# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test helper methods that check constraints on database objects."""

import pytest

import robcore.error as err
import robcore.model.constraint as constraint
import robcore.tests.db as db
import robcore.util as util


class TestDatabaseConstraints(object):
    """Unut test to verfiy that the constraint checker works properly."""
    def test_validate_name(self, tmpdir):
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
        # - Duplicate name
        sql = 'SELECT name FROM api_user WHERE name = ?'
        with pytest.raises(err.ConstraintViolationError) as ex:
            constraint.validate_name(name=user_id, con=con, sql=sql)
            assert str(ex) == 'name \'{}\' exists'.format(user_id)
