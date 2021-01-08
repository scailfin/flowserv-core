# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API methods."""

import pytest

from flowserv.config import Config, FLOWSERV_FILESTORE_MODULE, FLOWSERV_FILESTORE_CLASS
from flowserv.model.files.factory import FS
from flowserv.model.files.s3 import BucketStore
from flowserv.tests.files import DiskBucket

import flowserv.error as err
import flowserv.view.user as labels


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


def test_authenticate_user(local_service):
    """Test authenticating a user via the access token when initializing the API."""
    with local_service() as api:
        api.users().register_user(username='alice', password='abc', verify=False)
        doc = api.users().login_user(username='alice', password='abc')
        user_id = doc[labels.USER_ID]
        access_token = doc[labels.USER_TOKEN]
    local_service.set_access_token(token=access_token)
    with local_service() as api:
        assert api.runs().user_id == user_id
    # Ensure that no error is raised if an invalid access token is given.
    local_service.set_access_token(token='UNKNOWN')
    with local_service() as api:
        assert api.runs().user_id is None


def test_initialize_filestore_from_env(tmpdir):
    """Test initializing the bucket store with a memory bucket from the
    envirnment variables.
    """
    # -- Setup ----------------------------------------------------------------
    env = Config().basedir(tmpdir)
    env[FLOWSERV_FILESTORE_MODULE] = 'flowserv.model.files.s3'
    env[FLOWSERV_FILESTORE_CLASS] = 'BucketStore'
    # -- Create bucket store instance -----------------------------------------
    fs = FS(env=env)
    assert isinstance(fs, BucketStore)
    assert isinstance(fs.bucket, DiskBucket)
    # -- Error cases ----------------------------------------------------------
    del env[FLOWSERV_FILESTORE_MODULE]
    with pytest.raises(err.MissingConfigurationError):
        FS(env=env)
    env[FLOWSERV_FILESTORE_MODULE] = 'flowserv.model.files.s3'
    del env[FLOWSERV_FILESTORE_CLASS]
    with pytest.raises(err.MissingConfigurationError):
        FS(env=env)
    # -- Default file store ---------------------------------------------------
    assert FS(env=Config().basedir(tmpdir)) is not None
    with pytest.raises(err.MissingConfigurationError):
        FS(env=Config())
