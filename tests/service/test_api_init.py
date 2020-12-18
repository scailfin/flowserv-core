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

from flowserv.model.files.s3 import BucketStore, FLOWSERV_S3BUCKET
from flowserv.service.files.base import get_filestore
from flowserv.tests.files import DiskBucket

import flowserv.config.files as config
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
    with local_service() as api:
        assert api.runs().user_id is None
    with local_service(access_token=access_token) as api:
        assert api.runs().user_id == user_id
    # Error when providing an invalid combination of user_id and access_token.
    with pytest.raises(ValueError):
        with local_service(user_id='UNKNOWN', access_token=access_token) as api:
            pass
    # Ensure that no error is raised if an invalid access token is given.
    with local_service(access_token='UNKNOWN') as api:
        assert api.runs().user_id is None


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
    assert isinstance(fs.bucket, DiskBucket)
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
