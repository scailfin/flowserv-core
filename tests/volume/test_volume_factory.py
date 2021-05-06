# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for edge cases for the file store factory."""

import os
import pytest

from flowserv.config import FLOWSERV_BASEDIR
from flowserv.volume.factory import FStore, GCBucket, S3Bucket, Sftp, VolumeConfig, Volume
from flowserv.volume.fs import FileSystemStorage
from flowserv.volume.gc import GCVolume
from flowserv.volume.s3 import S3Volume
from flowserv.volume.ssh import RemoteStorage

import flowserv.error as err


def test_fs_volume(tmpdir):
    """Test instantiating the file system storage volume."""
    doc = FStore(basedir='0000', name='FS')
    assert doc == {'type': 'fs', 'args': {'basedir': '0000'}, 'name': 'FS'}
    del doc['args']
    basedir = os.path.join(tmpdir, 'fs')
    volume = Volume(doc, env={FLOWSERV_BASEDIR: basedir})
    assert volume.identifier == 'FS'
    assert volume.basedir == basedir
    assert isinstance(volume, FileSystemStorage)


def test_invalid_storage_type():
    """Test error when providing an invalid storage volume type identifier."""
    with pytest.raises(err.InvalidConfigurationError):
        Volume({'type': 'unknown'})


def test_gc_volume(mock_gcstore):
    """Test instantiating the Google Cloud storage volume."""
    doc = GCBucket(bucket='0000', name='GC')
    assert doc == {'type': 'gc', 'args': {'bucket': '0000'}, 'name': 'GC'}
    volume = Volume(doc)
    assert volume.identifier == 'GC'
    assert isinstance(volume, GCVolume)
    # -- Error for missing bucket identifier.
    del doc['args']['bucket']
    with pytest.raises(err.MissingConfigurationError):
        Volume(doc)


def test_s3_volume(mock_boto):
    """Test instantiating the S3 storage volume."""
    doc = S3Bucket(bucket='0000', name='S3')
    assert doc == {'type': 's3', 'args': {'bucket': '0000'}, 'name': 'S3'}
    volume = Volume(doc)
    assert volume.identifier == 'S3'
    assert isinstance(volume, S3Volume)
    # -- Error for missing bucket identifier.
    del doc['args']['bucket']
    with pytest.raises(err.MissingConfigurationError):
        Volume(doc)


def test_sftp_volume(mock_ssh, tmpdir):
    """Test generating the SFTP storage volume."""
    doc = Sftp(remotedir='/dev/null', hostname='myhost')
    assert doc == {'type': 'sftp', 'args': {'basedir': '/dev/null', 'hostname': 'myhost'}}
    basedir = os.path.join(tmpdir, 'ssh')
    doc = Sftp(
        remotedir=basedir,
        hostname='myhost',
        port=8088,
        timeout=5.5,
        look_for_keys=True,
        sep='$',
        name='SFTPVolume'
    )
    assert doc == {
        'type': 'sftp',
        'args': {
            'basedir': basedir,
            'hostname': 'myhost',
            'port': 8088,
            'timeout': 5.5,
            'lookForKeys': True,
            'seperator': '$'
        },
        'name': 'SFTPVolume'
    }
    volume = Volume(doc)
    assert volume.identifier == 'SFTPVolume'
    assert isinstance(volume, RemoteStorage)
    # -- Error for missing configuration parameters.
    del doc['args']['hostname']
    with pytest.raises(err.MissingConfigurationError):
        Volume(doc)
    del doc['args']['basedir']
    with pytest.raises(err.MissingConfigurationError):
        Volume(doc)


def test_volume_config_name():
    """Test storage volume configurations with and without storage identifier."""
    assert VolumeConfig(type='X', args={'a': 1}) == {'type': 'X', 'args': {'a': 1}}
    assert VolumeConfig(type='X', args={}, name='Z') == {'type': 'X', 'args': {}, 'name': 'Z'}
