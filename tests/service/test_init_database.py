# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests updating the global database object variable."""

import os

from flowserv.config.database import FLOWSERV_DB


def test_set_global_database(tmpdir):
    """Test updating the global service database variable based on the
    current environment settings.
    """
    # Create a new database.
    dbfile = '{}/test.db'.format(tmpdir)
    os.environ[FLOWSERV_DB] = 'sqlite:///{}'.format(dbfile)
    assert not os.path.isfile(dbfile)
    from flowserv.service.database import database
    database.init()
    assert database._engine.table_names() != []
    assert os.path.isfile(dbfile)
    # Change database configuration.
    dbfile = '{}/test2.db'.format(tmpdir)
    os.environ[FLOWSERV_DB] = 'sqlite:///{}'.format(dbfile)
    assert not os.path.isfile(dbfile)
    from flowserv.service.database import config_db
    config_db()
    from flowserv.service.database import database
    assert database._engine.table_names() == []
    database.init()
    assert database._engine.table_names() != []
    assert os.path.isfile(dbfile)
