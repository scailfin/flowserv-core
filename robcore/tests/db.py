# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to create a new instance of the database."""

import os

from robcore.config.install import DB

import robcore.config.db as config
import robcore.core.db.driver as driver
import robcore.core.db.sqlite as sqlite


def init_db(base_dir):
    """Create a fresh database with three users and return an open
    connection to the database. Returns connector to the database.

    Returns
    -------
    robcore.core.db.connector.DatabaseConnector
    """
    dbms_id = driver.SQLITE[0]
    connect_string = '{}/{}'.format(base_dir, 'tmp.db')
    DB.init(dbms_id=dbms_id, connect_string=connect_string)
    os.environ[config.ROB_DB_ID] = dbms_id
    os.environ[sqlite.SQLITE_ROB_CONNECT] = connect_string
    return driver.DatabaseDriver.get_connector()
