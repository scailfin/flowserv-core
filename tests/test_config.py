# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for configuration management functions."""

import os
import pytest

import flowserv.config as config
import flowserv.error as err
import flowserv.util as util


@pytest.mark.parametrize(
    'var,value,result',
    [
        (config.FLOWSERV_BASEDIR, 'DIR', 'DIR'),
        (config.FLOWSERV_API_HOST, 'HOST', 'HOST'),
        (config.FLOWSERV_API_NAME, 'NAME', 'NAME'),
        (config.FLOWSERV_API_PATH, 'PATH', 'PATH'),
        (config.FLOWSERV_API_PORT, '8888', 8888),
        (config.FLOWSERV_API_PORT, 'ABC', None),
        (config.FLOWSERV_API_PROTOCOL, 'PROTOCOL', 'PROTOCOL'),
        (config.FLOWSERV_APP, 'APP', 'APP'),
        (config.FLOWSERV_AUTH_LOGINTTL, '1234', 1234),
        (config.FLOWSERV_AUTH, 'AUTH', 'AUTH'),
        (config.FLOWSERV_BACKEND_CLASS, 'CLASS', 'CLASS'),
        (config.FLOWSERV_BACKEND_MODULE, 'MODULE', 'MODULE'),
        (config.FLOWSERV_RUNSDIR, 'DIR', 'DIR'),
        (config.FLOWSERV_POLL_INTERVAL, '2.3', 2.3),
        (config.FLOWSERV_POLL_INTERVAL, '20', 20.0),
        (config.FLOWSERV_POLL_INTERVAL, 'ABC', None),
        (config.FLOWSERV_ACCESS_TOKEN, 'TOKEN', 'TOKEN'),
        (config.FLOWSERV_CLIENT, 'CLIENT', 'CLIENT'),
        (config.FLOWSERV_DB, 'DB', 'DB'),
        (config.FLOWSERV_WEBAPP, 'True', True),
        (config.FLOWSERV_WEBAPP, 'true', True),
        (config.FLOWSERV_WEBAPP, 'TruE', True),
        (config.FLOWSERV_WEBAPP, 'False', False),
        (config.FLOWSERV_WEBAPP, 'ABC', False),
        (config.FLOWSERV_WEBAPP, '1', False),
        (config.FLOWSERV_FILESTORE, 'CLASS', 'CLASS'),
    ]
)
def test_config_env(var, value, result):
    """Test getting configuration values from environment variables."""
    os.environ[var] = value
    conf = config.env()
    assert conf[var] == result
    del os.environ[var]


def test_config_init():
    """Test initializing the config dictionary."""
    env = config.Config({'A': 1, 'B': 2})
    assert env['A'] == 1
    assert env['B'] == 2


def test_config_setter():
    """Test setter methods for configuration parameters."""
    conf = config.Config()
    # Default authentication.
    conf = conf.auth()
    assert conf[config.FLOWSERV_AUTH] == config.AUTH_DEFAULT
    # base directory
    conf = conf.basedir('/dev/null')
    assert conf[config.FLOWSERV_BASEDIR] == os.path.abspath('/dev/null')
    # Database
    conf.database('mysql')
    assert conf[config.FLOWSERV_DB] == 'mysql'
    # Open access
    conf = conf.open_access()
    assert conf[config.FLOWSERV_AUTH] == config.AUTH_OPEN
    # Workflow engine
    conf = conf.multiprocess_engine()
    assert conf[config.FLOWSERV_BACKEND_MODULE] == 'flowserv.controller.serial.engine.base'
    assert conf[config.FLOWSERV_BACKEND_CLASS] == 'SerialWorkflowEngine'
    # Sync engine
    conf = conf.run_sync()
    assert not conf[config.FLOWSERV_ASYNC]
    conf = conf.run_async()
    assert conf[config.FLOWSERV_ASYNC]
    # S3 bucket
    conf = conf.volume({'type': 'mybucket'})
    assert conf[config.FLOWSERV_FILESTORE] == {'type': 'mybucket'}
    # Token timeout
    conf = conf.token_timeout(100)
    assert conf[config.FLOWSERV_AUTH_LOGINTTL] == 100
    # Webapp
    conf = conf.webapp()
    assert conf[config.FLOWSERV_WEBAPP]


def test_config_url():
    """Test method to get the API base URL."""
    # Clear environment variable if set
    default_url = '{}://{}:{}{}'.format(
        config.DEFAULT_PROTOCOL,
        config.DEFAULT_HOST,
        config.DEFAULT_PORT,
        config.DEFAULT_PATH
    )
    conf = config.Config()
    assert config.API_URL(conf) == default_url
    conf[config.FLOWSERV_API_PORT] = '80'
    conf[config.FLOWSERV_API_PATH] = 'app-path/v1'
    api_url = '{}://{}/{}'.format(
        config.DEFAULT_PROTOCOL,
        config.DEFAULT_HOST,
        'app-path/v1'
    )
    assert config.API_URL(conf) == api_url


def test_engine_config(tmpdir):
    """Test worker configuration for the serial workflow controller."""
    conf = config.env()
    assert conf.get(config.FLOWSERV_ENGINECONFIG) is None
    doc = {'volumes': []}
    conf.engine_config(doc=doc)
    assert conf.get(config.FLOWSERV_ENGINECONFIG) == doc
    filename = os.path.join(tmpdir, 'config.json')
    util.write_object(filename=filename, obj=doc)
    os.environ[config.FLOWSERV_ENGINECONFIG] = filename
    conf = config.env()
    assert conf.get(config.FLOWSERV_ENGINECONFIG) == doc
    del os.environ[config.FLOWSERV_ENGINECONFIG]


def test_engine_rundirectory():
    """Test default run directory path."""
    assert config.RUNSDIR(dict({config.FLOWSERV_RUNSDIR: 'abc'})) == 'abc'
    assert config.RUNSDIR(dict()) is not None


def test_env_app_identifier():
    """Test getting the workflow identifier and sbmission identifier from the
    environment.
    """
    os.environ[config.FLOWSERV_APP] = '0000'
    assert config.APP() == '0000'
    del os.environ[config.FLOWSERV_APP]
    with pytest.raises(err.MissingConfigurationError):
        config.APP()


@pytest.mark.parametrize('value,result', [(1, False), ('true', True), ('false', False)])
def test_to_bool(value, result):
    """Test the to boolean converter."""
    config.to_bool(value) == result
