# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the Google Cloud storage volume."""

import json
import pytest

from flowserv.tests.files import io_file
from flowserv.volume.gc import GCFile, GCVolume

import flowserv.error as err


FILES = [
    (io_file(['Alice', 'Bob']), 'data/names.txt'),
    (io_file({'a': 1}), 'code/obj1.json'),
    (io_file({'b': 2}), 'code/obj2.json')
]


@pytest.fixture
def store(mock_gcstore):
    volume = GCVolume(bucket_name='GCB01', identifier='V0001')
    assert volume.identifier == 'V0001'
    assert volume.bucket_name == 'GCB01'
    assert 'GCB01' in volume.describe()
    for buf, key in FILES:
        volume.store(dst=key, file=buf)
    return volume


def test_gc_delete_file(store):
    """Test deleting existing files."""
    store.delete(keys=['data/names.txt'])
    f = GCFile(key='data/names.txt', client=store.client, bucket_name=store.bucket_name)
    with pytest.raises(err.UnknownFileError):
        f.open()
    # Ensure that there is no error when deleting a non-existing file.
    store.delete(keys=['data/names.txt'])


def test_gc_erase_bucket(store):
    """Test erasing all files in a Google Cloud storage bucket."""
    store.erase()
    assert len(store.client.buckets[store.bucket_name].objects) == 0
    store.close()


def test_gc_load_file(store):
    """Test loading and reading a GCFile handle object."""
    f = store.load(key='data/names.txt')
    with f.open() as b:
        doc = json.load(b)
    assert doc == ['Alice', 'Bob']
    assert f.size() > 0


def test_gc_query_files(store):
    """Test querying a Google Cloud bucket store."""
    assert store.walk(src=None) == set([key for _, key in FILES])
    assert store.walk(src='code') == set({'code/obj1.json', 'code/obj2.json'})
    assert store.walk(src='code/') == set({'code/obj1.json', 'code/obj2.json'})


def test_gc_open_file(store):
    """Error case when openening an unknown file."""
    f = GCFile(key='unknown', client=store.client, bucket_name=store.bucket_name)
    with pytest.raises(err.UnknownFileError):
        f.open()


def test_gc_store_init(mock_gcstore):
    """Error case when initializing the GC bucket store without a bucket
    identifier.
    """
    # Initialize with existing bucket.
    volume = GCVolume(bucket_name='test_exists')
    assert volume.bucket_name == 'test_exists'
