# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to create a new instance of the database."""

import os

from flowserv.config.install import DB

import flowserv.config.db as config
import flowserv.core.db.driver as driver
import flowserv.core.db.sqlite as sqlite


def init_db(basedir):
    """Create a fresh database with three users and return an open
    connection to the database. Returns connector to the database.

    Returns
    -------
    flowserv.core.db.connector.DatabaseConnector
    """
    dbms_id = driver.SQLITE[0]
    connect_string = '{}/{}'.format(basedir, 'tmp.db')
    DB.init(dbms_id=dbms_id, connect_string=connect_string)
    os.environ[config.ROB_DB_ID] = dbms_id
    os.environ[sqlite.SQLITE_ROB_CONNECT] = connect_string
    return driver.DatabaseDriver.get_connector()
