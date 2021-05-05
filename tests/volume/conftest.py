# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Fixtures for volume storage unit tests."""

import boto3
import botocore
import os
import pytest

import flowserv.util as util


"""Test data."""
DATA_A = {'A': 1}
DATA_B = {'B': 2}
DATA_C = {'C': 3}
DATA_D = {'D': 4}
DATA_E = [DATA_A, DATA_B]

FILE_A = 'A.json'
FILE_B = os.path.join('examples', 'B.json')
FILE_C = os.path.join('examples', 'C.json')
FILE_D = os.path.join('docs', 'D.json')
FILE_E = os.path.join('examples', 'data', 'data.json')


@pytest.fixture
def basedir(tmpdir):
    """Create the following file structure in a given base directory for unit
    tests:

      A.json                   -> DATA_A
      docs/D.json              -> DATA_D
      examples/B.json          -> DATA_B
      examples/C.json          -> DATA_C
      examples/data/data.json  -> DATA_E

    Returns the base directory containing the created files.
    """
    tmpdir = os.path.join(tmpdir, 'inputs')
    os.makedirs(tmpdir)
    # A.json
    fileA = os.path.join(tmpdir, FILE_A)
    util.write_object(obj=DATA_A, filename=fileA)
    # examples/B.json
    fileB = os.path.join(tmpdir, FILE_B)
    os.makedirs(os.path.dirname(fileB))
    util.write_object(obj=DATA_B, filename=fileB)
    # examples/C.json
    fileC = os.path.join(tmpdir, FILE_C)
    util.write_object(obj=DATA_C, filename=fileC)
    # examples/data/data.json
    fileE = os.path.join(tmpdir, FILE_E)
    os.makedirs(os.path.dirname(fileE))
    util.write_object(obj=DATA_E, filename=fileE)
    # docs/D.json
    fileD = os.path.join(tmpdir, FILE_D)
    os.makedirs(os.path.dirname(fileD))
    util.write_object(obj=DATA_D, filename=fileD)
    return tmpdir


@pytest.fixture
def data_a():
    """Content for file 'A.json'."""
    return DATA_A


@pytest.fixture
def data_e():
    """Content for file 'examples/data/data.json'."""
    return DATA_E


@pytest.fixture
def emptydir(tmpdir):
    """Get reference to an empty output directory."""
    tmpdir = os.path.join(tmpdir, 'outputs')
    os.makedirs(tmpdir)
    return tmpdir


@pytest.fixture
def filenames_all():
    """Set of names for all files in the created base directory."""
    return {'A.json', 'examples/B.json', 'examples/C.json', 'docs/D.json', 'examples/data/data.json'}


class BlobObject:
    def __init__(self, key, bucket=None):
        self.key = key
        self.name = key
        self.bucket = bucket

    def download_as_bytes(self):
        if self.key not in self.bucket.objects:
            from google.cloud.exceptions import NotFound
            raise NotFound(self.key)
        buf = self.bucket.objects[self.key]
        buf.seek(0)
        return buf.read()

    def upload_from_file(self, fh):
        self.bucket.objects[self.key] = fh


# -- AWS S3 -------------------------------------------------------------------

class MockS3Bucket:
    def __init__(self):
        self.bucket = dict()

    def Bucket(self, identifier):
        return self

    def delete_objects(self, Delete):
        for obj in Delete.get('Objects', []):
            del self.bucket[obj['Key']]

    def download_fileobj(self, key, data):
        try:
            buf = self.bucket[key]
        except KeyError:
            raise botocore.exceptions.ClientError(error_response={}, operation_name='mock')
        buf.seek(0)
        data.write(buf.read())

    def filter(self, Prefix):
        if Prefix:
            keys = [k for k in self.bucket if k.startswith(Prefix)]
        else:
            keys = list(self.bucket.keys())
        return [BlobObject(k) for k in keys]

    @property
    def objects(self):
        return self

    def upload_fileobj(self, buf, key):
        self.bucket[key] = buf


@pytest.fixture
def mock_boto(monkeypatch):
    """Replace boto3.resource with test resource object."""

    def mock_s3_bucket(*args, **kwargs):
        return MockS3Bucket()

    monkeypatch.setattr(boto3, "resource", mock_s3_bucket)


# -- Google Cloud File Storage ------------------------------------------------

class BlobBucket:
    def __init__(self, name):
        self.name = name
        self.objects = dict()

    def blob(self, key):
        return BlobObject(key, bucket=self)

    def delete_blobs(self, keys):
        for key in keys:
            if key not in self.objects:
                from google.cloud.exceptions import NotFound
                raise NotFound(key)
            del self.objects[key]

    def query(self, prefix):
        if prefix:
            keys = [k for k in self.objects if k.startswith(prefix)]
        else:
            keys = list(self.objects.keys())
        return keys


class GCClient:
    def __init__(self):
        self.buckets = dict({'test_exists': BlobBucket('test_exists')})

    def bucket(self, name):
        return self.buckets[name]

    def create_bucket(self, name):
        if name not in self.buckets:
            self.buckets[name] = BlobBucket(name)

    def list_blobs(self, name, prefix):
        return [BlobObject(b) for b in self.bucket(name).query(prefix)]

    def list_buckets(self):
        return self.buckets.values()


@pytest.fixture
def mock_gcstore(monkeypatch):
    """Replace storage.Client with test Client object."""

    def mock_gc_client(*args, **kwargs):
        return GCClient()

    from flowserv.volume import gc
    monkeypatch.setattr(gc, "get_google_client", mock_gc_client)
