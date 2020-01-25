# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the database driver."""

import os
import pytest

from flowserv.core.db.connector import DatabaseConnector
from flowserv.core.db.driver import DatabaseDriver as DB

import flowserv.config.db as config
import flowserv.core.db.sqlite as sqlite


class TestDatabaseDriver(object):
    """Collection of unit tests for the database driver."""
    def test_interface(self):
        """Test abstract interface methods to ensure that they raise
        NotImplementedError.
        """
        connector = DatabaseConnector()
        with pytest.raises(NotImplementedError):
            connector.connect()
        with pytest.raises(NotImplementedError):
            connector.info()
        with pytest.raises(NotImplementedError):
            connector.execute(schema_file='/dev/null')

    def test_connect_sqlite(self, tmpdir):
        """Test instantiating database connectors."""
        # SQLite
        f_name = '/tmp/test.db'
        os.environ[config.FLOWSERV_DB_ID] = 'SQLITE'
        os.environ[sqlite.SQLITE_FLOWSERV_CONNECT] = f_name
        db = DB.get_connector()
        assert db.info(indent='..') == '..sqlite3 {}'.format(f_name)
        f_name = '/tmp/test.sqlite3.db'
        db = DB.get_connector(dbms_id='SQLITE3', connect_string=f_name)
        assert db.info(indent='..') == '..sqlite3 {}'.format(f_name)
        # PostgreSQL
        connect = 'localhost:5678/mydb:myuser/the/pwd'
        os.environ[config.FLOWSERV_DB_ID] = 'POSTGRES'
        db = DB.get_connector(connect_string=connect)
        info_str = 'postgres {} on {}'.format('mydb', 'localhost:5678')
        assert db.info() == info_str
        assert db.user == 'myuser'
        assert db.password == 'the/pwd'
        db = DB.get_connector(connect_string='localhost/db:user/some:pwd')
        assert db.info() == 'postgres {} on {}'.format('db', 'localhost')
        assert db.user == 'user'
        assert db.password == 'some:pwd'
        # Unknown database identifier
        with pytest.raises(ValueError):
            DB.get_connector(dbms_id='unknown', connect_string='CONNECT ME')
