# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for helper methods that are used to configure the serial
workflow engine.
"""

from jsonschema.exceptions import ValidationError

import os
import pytest

import flowserv.controller.serial.engine.config as config

# Config files.
DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(DIR, '../../../.files/controller')
VALID_CONFIG = os.path.join(CONFIG_DIR, 'worker.json')
INALID_CONFIG = os.path.join(CONFIG_DIR, 'worker.yaml')


def test_engine_config(tmpdir):
    """Test worker configuration for the serial workflow controller."""
    # Missing parameter value.
    assert config.ENGINECONFIG(env=dict()) == {}
    # Read object from file.
    env = {config.FLOWSERV_SERIAL_ENGINECONFIG: VALID_CONFIG}
    assert config.ENGINECONFIG(env=env) is not None
    # Read invalid configuration file.
    env = {config.FLOWSERV_SERIAL_ENGINECONFIG: INALID_CONFIG}
    assert config.ENGINECONFIG(env=env) is not None
    with pytest.raises(ValidationError):
        config.ENGINECONFIG(env=env, validate=True)


def test_engine_rundirectory():
    """Test default run directory path."""
    assert config.RUNSDIR(dict({config.FLOWSERV_SERIAL_RUNSDIR: 'abc'})) == 'abc'
    assert config.RUNSDIR(dict()) is not None
