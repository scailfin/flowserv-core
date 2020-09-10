# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API methods."""

import os
import pytest

from flowserv.model.auth import DefaultAuthPolicy
from flowserv.model.files.s3 import BucketStore, FLOWSERV_S3BUCKET
from flowserv.service.files import get_filestore
from flowserv.tests.files import MemBucket

import flowserv.config.files as config
import flowserv.error as err


def test_api_components(service):
    """Test methods to access API components."""
    with service() as api:
        # The API uses the default authentication handler.
        assert isinstance(api.auth, DefaultAuthPolicy)
        # Error when authenticating unknown user.
        with pytest.raises(err.UnauthenticatedAccessError):
            api.authenticate('0000')
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
    os.environ[config.FLOWSERV_FILESTORE_MODULE] = 'flowserv.model.files.s3'
    os.environ[config.FLOWSERV_FILESTORE_CLASS] = 'BucketStore'
    if FLOWSERV_S3BUCKET in os.environ:
        del os.environ[FLOWSERV_S3BUCKET]
    # -- Create bucket store instance -----------------------------------------
    fs = get_filestore()
    assert isinstance(fs, BucketStore)
    assert isinstance(fs.bucket, MemBucket)
    # -- Error cases ----------------------------------------------------------
    del os.environ[config.FLOWSERV_FILESTORE_MODULE]
    with pytest.raises(err.MissingConfigurationError):
        get_filestore()
    assert get_filestore(raise_error=False) is None
    os.environ[config.FLOWSERV_FILESTORE_MODULE] = 'flowserv.model.files.s3'
    del os.environ[config.FLOWSERV_FILESTORE_CLASS]
    with pytest.raises(err.MissingConfigurationError):
        get_filestore()
    # -- Default file store ---------------------------------------------------
    del os.environ[config.FLOWSERV_FILESTORE_MODULE]
    os.environ[config.FLOWSERV_API_BASEDIR] = str(tmpdir)
    assert get_filestore() is not None
    del os.environ[config.FLOWSERV_API_BASEDIR]
