# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the SQLite database connector."""

import os

import flowserv.tests.db as db


DIR = os.path.dirname(os.path.realpath(__file__))
DML_FILE = os.path.join(DIR, '../../.files/db/testdb.sql')


class TestSQLiteConnector(object):
    """Unit tests for the SQLite database connector to test the connect() and
    init_db() methods.
    """
    def test_create_db(self, tmpdir):
        """Use the database driver init_db method to test connecting to a SQLite
        database and executing a script.
        """
        connector = db.init_db(str(tmpdir))
        connector.execute(DML_FILE)
        with connector.connect() as con:
            rs = con.execute('SELECT * FROM workflow_template').fetchall()
            assert len(rs) == 2
            assert rs[0]['workflow_id'] == '1234'
            assert rs[0]['name'] == 'Alice'
            assert rs[1]['workflow_id'] == '5678'
            assert rs[1]['name'] == 'Bob'
            # Ensure that LIKE is case sensitive
            sql = 'SELECT * FROM workflow_template WHERE name LIKE \'a%\''
            assert len(con.execute(sql).fetchall()) == 0
            sql = 'SELECT * FROM workflow_template WHERE name LIKE \'A%\''
            assert len(con.execute(sql).fetchall()) == 1
