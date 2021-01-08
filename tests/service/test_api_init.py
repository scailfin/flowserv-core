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


def test_api_components(local_service, tmpdir):
    """Test methods to access API components."""
    config = Config().basedir(tmpdir)
    with local_service(config=config) as api:
        # Access the different managers to ensure that they are created
        # properly without raising errors.
        assert api.groups() is not None
        assert api.runs() is not None
        assert api.server() is not None
        assert api.uploads() is not None
        assert api.users() is not None
        assert api.workflows() is not None


def test_authenticate_user(local_service, tmpdir):
    """Test authenticating a user via the access token when initializing the API."""
    config = Config().basedir(tmpdir)
    with local_service(config=config) as api:
        api.users().register_user(username='alice', password='abc', verify=False)
        doc = api.users().login_user(username='alice', password='abc')
        user_id = doc[labels.USER_ID]
        access_token = doc[labels.USER_TOKEN]
    with local_service(config=config) as api:
        assert api.runs().user_id is None
    with local_service(config=config, access_token=access_token) as api:
        assert api.runs().user_id == user_id
    # Error when providing an invalid combination of user_id and access_token.
    with pytest.raises(ValueError):
        with local_service(config=config, user_id='UNKNOWN', access_token=access_token) as api:
            pass
    # Ensure that no error is raised if an invalid access token is given.
    with local_service(config=config, access_token='UNKNOWN') as api:
        assert api.runs().user_id is None


def test_initialize_filestore_from_env(tmpdir):
    """Test initializing the bucket store with a memory bucket from the
    envirnment variables.
    """
    # -- Setup ----------------------------------------------------------------
    config = Config().basedir(tmpdir)
    config[FLOWSERV_FILESTORE_MODULE] = 'flowserv.model.files.s3'
    config[FLOWSERV_FILESTORE_CLASS] = 'BucketStore'
    # -- Create bucket store instance -----------------------------------------
    fs = FS(config=config)
    assert isinstance(fs, BucketStore)
    assert isinstance(fs.bucket, DiskBucket)
    # -- Error cases ----------------------------------------------------------
    del config[FLOWSERV_FILESTORE_MODULE]
    with pytest.raises(err.MissingConfigurationError):
        FS(config=config)
    config[FLOWSERV_FILESTORE_MODULE] = 'flowserv.model.files.s3'
    del config[FLOWSERV_FILESTORE_CLASS]
    with pytest.raises(err.MissingConfigurationError):
        FS(config=config)
    # -- Default file store ---------------------------------------------------
    assert FS(config=Config().basedir(tmpdir)) is not None
    with pytest.raises(err.MissingConfigurationError):
        FS(config=Config())
