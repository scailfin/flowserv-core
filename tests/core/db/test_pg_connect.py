# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the PostgreSQL database connector."""

import pytest

from robcore.core.db.pg import PostgresConnector


class TestPostgreSQLConnector(object):
    """The unit tests for the PostgreSQL database connector currently focus only
    on parsing the connection string.
    """
    def test_connect_string(self):
        """Test parsing connect strings."""
        db = PostgresConnector(connect_string='localhost/db:user/some:pwd')
        assert db.host == 'localhost'
        assert db.database == 'db'
        assert db.user == 'user'
        assert db.password == 'some:pwd'
        with pytest.raises(ValueError):
            PostgresConnector(connect_string='some string')
        with pytest.raises(ValueError):
            PostgresConnector(connect_string='localhost/db/user/somepwd')
        with pytest.raises(ValueError):
            PostgresConnector(connect_string='localhost/db:user-somepwd')
