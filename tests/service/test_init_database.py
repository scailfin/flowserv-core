# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests updating the global database object variable."""

import os
import pytest

from flowserv.config import Config, FLOWSERV_DB

import flowserv.error as err


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
    assert not os.path.isfile(dbfile)
    from flowserv.service.database import init_db
    init_db(Config().database('sqlite:///{}'.format(dbfile)))
    from flowserv.service.database import database
    assert database._engine.table_names() == []
    database.init()
    assert database._engine.table_names() != []
    assert os.path.isfile(dbfile)
    # -- Error for missing database url configuration.
    del os.environ[FLOWSERV_DB]
    with pytest.raises(err.MissingConfigurationError):
        init_db()
