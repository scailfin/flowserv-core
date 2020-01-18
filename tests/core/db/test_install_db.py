# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test creating a new database using the install scripts that are included in
the package.
"""

import robcore.tests.db as db


class TestInstallDatabase(object):
    """Unit test for creating a clean database instance."""
    def test_install(self, tmpdir):
        """Create clean database instance using SQLite3."""
        connector = db.init_db(str(tmpdir))
        with connector.connect() as con:
            sql = 'SELECT * FROM api_user'
            assert len(con.execute(sql).fetchall()) == 0
