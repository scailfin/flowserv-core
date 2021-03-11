# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API methods."""

import pytest

from flowserv.config import Config
from flowserv.config import FLOWSERV_FILESTORE, FLOWSERV_FILESTORE_BUCKETTYPE
from flowserv.model.files.factory import FS
from flowserv.model.files.bucket import BucketStore
from flowserv.tests.files import DiskBucket

import flowserv.error as err


def test_api_components(local_service):
    """Test methods to access API components."""
    with local_service() as api:
        # Access the different managers to ensure that they are created
        # properly without raising errors.
        assert api.groups() is not None
        assert api.runs() is not None
        assert api.server() is not None
        assert api.uploads() is not None
        assert api.users() is not None
        assert api.workflows() is not None


def test_initialize_filestore_from_env(tmpdir):
    """Test initializing the bucket store with a memory bucket from the
    envirnment variables.
    """
    # -- Setup ----------------------------------------------------------------
    env = Config().basedir(tmpdir)
    env[FLOWSERV_FILESTORE] = 'bucket'
    env[FLOWSERV_FILESTORE_BUCKETTYPE] = 'test'
    # -- Create bucket store instance -----------------------------------------
    fs = FS(env=env)
    assert isinstance(fs, BucketStore)
    assert isinstance(fs.bucket, DiskBucket)
    # -- Error cases ----------------------------------------------------------
    del env[FLOWSERV_FILESTORE_BUCKETTYPE]
    with pytest.raises(err.MissingConfigurationError):
        FS(env=env)
    # -- Default file store ---------------------------------------------------
    assert FS(env=Config().basedir(tmpdir)) is not None
    with pytest.raises(err.MissingConfigurationError):
        FS(env=Config())
