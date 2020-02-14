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

from flowserv.core.db.driver import DatabaseDriver as DB

import flowserv.config.db as config
import flowserv.core.db.driver as driver
import flowserv.core.db.sqlite as sqlite


def test_configuration():
    """Test methods that get connector-specific configuration variables."""
    configuration = DB.configuration(dbms_id=driver.SQLITE[0])
    keys = [c[0] for c in configuration]
    assert len(keys) == 2
    assert config.FLOWSERV_DB_ID in keys
    assert sqlite.SQLITE_FLOWSERV_CONNECT in keys
    os.environ[config.FLOWSERV_DB_ID] = driver.POSTGRES[0]
    configuration = DB.configuration()
    assert len(configuration) == 6
    del os.environ[config.FLOWSERV_DB_ID]
    # No error if the database system identifier is not set or unknown
    configuration = DB.configuration()
    assert len(configuration) == 1
    assert configuration[0][1] == ''
    configuration = DB.configuration(dbms_id='unknown')
    assert len(configuration) == 1
    assert configuration[0][1] == 'unknown'


def test_connect_sqlite(tmpdir):
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
