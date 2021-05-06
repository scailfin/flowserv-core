# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for edge cases for the file store factory."""

import pytest

from flowserv.model.files.factory import FS

import flowserv.config as config
import flowserv.error as err


def test_invalid_configurations():
    """Test error cases for invalid configurations."""
    with pytest.raises(err.MissingConfigurationError):
        FS(env={config.FLOWSERV_FILESTORE: 'unknown'})
    with pytest.raises(err.MissingConfigurationError):
        FS(env={config.FLOWSERV_FILESTORE: config.FILESTORE_BUCKET})
    with pytest.raises(err.InvalidConfigurationError):
        FS(env={
            config.FLOWSERV_FILESTORE: config.FILESTORE_BUCKET,
            config.FLOWSERV_FILESTORE_BUCKETTYPE: 'unknown'
        })
