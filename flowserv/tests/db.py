# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to create a new instance of the database."""

import os

from passlib.hash import pbkdf2_sha256

from flowserv.config.install import DB

import flowserv.config.db as config
import flowserv.core.db.driver as driver
import flowserv.core.db.sqlite as sqlite


def init_db(basedir, workflows=None, users=None):
    """Create a fresh database. If a list of workflow identifier is given an
    entry for each of them in the workflow template table will be created.
    If a list of user identifier is given for each identifier a new user will
    be created that has the identifier as name and password.

    Returns a connector to the database.

    Parameters
    ----------
    basedir: string
        Directory for the database file.
    users: list(string), optional
        Optional list of user identifier

    Returns
    -------
    flowserv.core.db.connector.DatabaseConnector
    """
    dbms_id = driver.SQLITE[0]
    connect_string = '{}/{}'.format(basedir, 'tmp.db')
    DB.init(dbms_id=dbms_id, connect_string=connect_string)
    os.environ[config.FLOWSERV_DB_ID] = dbms_id
    os.environ[sqlite.SQLITE_FLOWSERV_CONNECT] = connect_string
    connector = driver.DatabaseDriver.get_connector()
    if users is not None:
        sql = (
            'INSERT INTO api_user(user_id, name, secret, active) '
            'VALUES(?, ?, ?, ?)'
        )
        with connector.connect() as con:
            for user_id in users:
                pwd = pbkdf2_sha256.hash(user_id)
                con.execute(sql, (user_id, user_id, pwd, 1))
            con.commit()
    if workflows is not None:
        sql = (
            'INSERT INTO workflow_template(workflow_id, name, workflow_spec) '
            'VALUES(?, ?, ?)'
        )
        with connector.connect() as con:
            for workflow_id in workflows:
                con.execute(sql, (workflow_id, workflow_id, '{}'))
            con.commit()
    return connector
