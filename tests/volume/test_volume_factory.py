# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for edge cases for the file store factory."""

import pytest

from flowserv.volume.factory import FStore, GCBucket, S3Bucket, Sftp, Volume
from flowserv.volume.fs import FileSystemStorage
from flowserv.volume.gc import GCVolume
from flowserv.volume.s3 import S3Volume
from flowserv.volume.ssh import RemoteStorage

import flowserv.error as err


def test_fs_volume(tmpdir):
    """Test instantiating the file system storage volume."""
    basedir = str(tmpdir)
    doc = FStore(basedir=basedir, identifier='FS')
    fs = Volume(doc)
    assert isinstance(fs, FileSystemStorage)
    assert fs.basedir == basedir
    assert fs.identifier == 'FS'


def test_invalid_storage_type():
    """Test error when providing an invalid storage volume type identifier."""
    with pytest.raises(err.InvalidConfigurationError):
        Volume({'type': 'unknown'})


def test_gc_volume(mock_gcstore):
    """Test instantiating the Google Cloud storage volume."""
    doc = GCBucket(bucket='0000', identifier='GC')
    fs = Volume(doc)
    assert isinstance(fs, GCVolume)
    assert fs.identifier == 'GC'


def test_s3_volume(mock_boto):
    """Test instantiating the S3 storage volume."""
    doc = S3Bucket(bucket='0000', identifier='S3')
    fs = Volume(doc)
    assert isinstance(fs, S3Volume)
    assert fs.identifier == 'S3'


def test_sftp_volume(mock_ssh, tmpdir):
    """Test generating the SFTP storage volume."""
    basedir = str(tmpdir)
    doc = Sftp(remotedir=basedir, hostname='myhost', identifier='SFTPVolume')
    fs = Volume(doc)
    assert isinstance(fs, RemoteStorage)
    assert fs.identifier == 'SFTPVolume'
