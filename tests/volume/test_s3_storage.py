# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the S3 bucket volume store."""

import json
import pytest

from flowserv.model.files import io_file
from flowserv.volume.s3 import S3File, S3Volume, S3_STORE

import flowserv.error as err


@pytest.fixture
def store(mock_boto):
    volume = S3Volume(bucket_id='S3B01', identifier='V0001')
    assert volume.bucket_id == 'S3B01'
    assert volume.identifier == 'V0001'
    assert 'S3B01' in volume.describe()
    return volume


def test_s3_erase_bucket(store):
    """Test erasing all files in a S3 bucket."""
    store.erase()
    assert len(store.bucket.bucket) == 0
    store.close()


def test_s3_load_file(store, people):
    """Test loading and reading a S3File handle object."""
    f = store.load(key='data/names.txt')
    with f.open() as b:
        doc = json.load(b)
    assert doc == people
    assert f.size() > 0


def test_s3_mkdir(store):
    """Test mkdir for completeness purposes."""
    store.mkdir(path='a/b/c')


def test_s3_query_files(store, bucket_keys):
    """Test querying a S3 bucket store."""
    assert store.walk(src=None) == bucket_keys
    assert store.walk(src='code') == set({'code/obj1.json', 'code/obj2.json'})
    assert store.walk(src='code/') == set({'code/obj1.json', 'code/obj2.json'})


def test_s3_open_file(store):
    """Error case when openening an unknown file."""
    f = S3File(key='unknown', bucket=store.bucket)
    with pytest.raises(err.UnknownFileError):
        f.open()


def test_s3_volume_serialization(mock_boto):
    """Test serialization for a S3 bucket storage object."""
    doc = S3Volume(identifier='0000', bucket_id='B0').to_dict()
    assert doc == {
        'type': S3_STORE,
        'id': '0000',
        'args': [{'key': 'bucket', 'value': 'B0'}, {'key': 'prefix', 'value': None}]
    }
    fs = S3Volume.from_dict(doc)
    assert isinstance(fs, S3Volume)
    assert fs.identifier == '0000'
    assert fs.bucket_id == 'B0'
    assert fs.prefix is None
    doc = S3Volume(identifier='0000', bucket_id='B1', prefix='dev').to_dict()
    assert doc == {
        'type': S3_STORE,
        'id': '0000',
        'args': [{'key': 'bucket', 'value': 'B1'}, {'key': 'prefix', 'value': 'dev'}]
    }
    fs = S3Volume.from_dict(doc)
    assert isinstance(fs, S3Volume)
    assert fs.identifier == '0000'
    assert fs.bucket_id == 'B1'
    assert fs.prefix == 'dev'


def test_s3_volume_subfolder(store, people):
    """Test creating a new storage volume for a sub-folder of the base directory
    of a S3 bucket storage volume.
    """
    substore = store.get_store_for_folder(key='data', identifier='SUBSTORE')
    assert substore.identifier == 'SUBSTORE'
    with substore.load(key='names.txt').open() as f:
        doc = json.load(f)
    assert doc == people
    # Store a file in the sub folder and then make sure we can read it.
    substore.store(file=io_file(['a', 'b']), dst='x/y')
    with substore.load(key='x/y').open() as f:
        doc = json.load(f)
    assert doc == ['a', 'b']
    # Erase all files in the sub-folder.
    substore.erase()
    # Note that the file will not have been deleted in the original store. This
    # is because of how the unit tests are set up with each store having its
    # own full list of the files.
    assert substore.walk(src=None) == set()
