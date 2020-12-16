# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

import pytest

from flowserv.model.database import DB, TEST_URL


@pytest.fixture
def database():
    """Create a fresh instance of the database."""
    db = DB(connect_url=TEST_URL, web_app=False)
    db.init()
    return db
