# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for different implementations of the bucket interface."""

import boto3
import botocore
import os
import pytest
import json

from flowserv.model.files.bucket import BucketStore
from flowserv.model.files.fs import DiskBucket
from flowserv.model.files.mem import MemBucket
from flowserv.model.files.s3 import S3Bucket
from flowserv.tests.files import io_file

import flowserv.config as config
import flowserv.error as err


FILES = [
    (io_file(['Alice', 'Bob']), 'data/names.txt'),
    (io_file({'a': 1}), 'code/obj1.json'),
    (io_file({'b': 2}), 'code/obj2.json')
]


class ObjectSummary:
    """Simple class to simulate object summaries. Only implements the .key
    property.
    """
    def __init__(self, key):
        """Initialize the object key."""
        self.key = key


class MockS3Bucket:
    def __init__(self):
        self.bucket = MemBucket()

    def Bucket(self, identifier):
        return self

    def delete_objects(self, Delete):
        keys = [obj['Key'] for obj in Delete.get('Objects', [])]
        self.bucket.delete(keys=keys)

    def download_fileobj(self, key, data):
        try:
            data.write(self.bucket.download(key).read())
        except err.UnknownFileError:
            raise botocore.exceptions.ClientError(error_response={}, operation_name='mock')

    def filter(self, Prefix):
        return [ObjectSummary(k) for k in self.bucket.query(Prefix)]

    @property
    def objects(self):
        return self

    def upload_fileobj(self, buf, key):
        self.bucket.objects[key] = buf


@pytest.fixture
def mock_boto(monkeypatch):
    """Raise error in subprocess.run()."""

    def mock_s3_bucket(*args, **kwargs):
        return MockS3Bucket()

    monkeypatch.setattr(boto3, "resource", mock_s3_bucket)


def test_disk_bucket(tmpdir):
    """Test functionality of the DiskBucket via the BucketStore."""
    run_test(bucket=DiskBucket(basedir=os.path.join(tmpdir, 'store')), basedir=tmpdir)


def test_mem_bucket(tmpdir):
    """Test functionality of the MemBucket via the BucketStore."""
    run_test(bucket=MemBucket(), basedir=tmpdir)


def test_s3_bucket(mock_boto, tmpdir):
    """Test functionality of the S3Buckey via the BucketStore."""
    run_test(bucket=S3Bucket({config.FLOWSERV_BUCKET: 'test'}), basedir=tmpdir)


# -- Generic test -------------------------------------------------------------

def run_test(bucket, basedir):
    """Run generic test for the given bucket instance."""
    store = BucketStore(bucket=bucket)
    # -- Store files ----------------------------------------------------------
    # Empty file list.
    store.store_files(files=[], dst='template')
    # Store test files
    store.store_files(files=FILES, dst='template')
    # -- Load file ------------------------------------------------------------
    f = store.load_file(key='template/data/names.txt')
    names = json.load(f.open())
    assert names == ['Alice', 'Bob']
    with pytest.raises(err.UnknownFileError):
        store.load_file(key='undefined.txt').open()
    # -- Copy folder ----------------------------------------------------------
    outdir = os.path.join(basedir, 'outputs')
    store.copy_folder(key='template/results', dst=outdir)
    assert os.listdir(outdir) == []
    store.copy_folder(key='template/data', dst=outdir)
    assert os.listdir(outdir) == ['names.txt']
    # -- Delete files ---------------------------------------------------------
    store.delete_file('undefined.txt')
    store.delete_file('template/data/names.txt')
    store.delete_folder('template/data')
    store.delete_folder('template/code')
