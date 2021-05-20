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

from flowserv.model.files import io_file
from flowserv.volume.gc import GCFile, GCVolume, GC_STORE

import flowserv.error as err


@pytest.fixture
def store(mock_gcstore):
    volume = GCVolume(bucket_name='GCB01', identifier='V0001')
    assert volume.identifier == 'V0001'
    assert volume.bucket_name == 'GCB01'
    assert 'GCB01' in volume.describe()
    return volume


def test_gc_delete_file(store):
    """Test deleting existing files."""
    store.delete(key='data/names.txt')
    f = GCFile(key='data/names.txt', client=store.client, bucket_name=store.bucket_name)
    with pytest.raises(err.UnknownFileError):
        f.open()
    # Ensure that there is no error when deleting a non-existing file.
    store.delete_objects(keys=['data/names.txt'])


def test_gc_erase_bucket(store):
    """Test erasing all files in a Google Cloud storage bucket."""
    store.erase()
    assert len(store.client.buckets[store.bucket_name].objects) == 0
    store.close()


def test_gc_load_file(store, people):
    """Test loading and reading a GCFile handle object."""
    f = store.load(key='data/names.txt')
    with f.open() as b:
        doc = json.load(b)
    assert doc == people
    assert f.size() > 0


def test_gc_query_files(store, bucket_keys):
    """Test querying a Google Cloud bucket store."""
    assert store.walk(src=None) == bucket_keys
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


def test_gc_volume_serialization(mock_gcstore):
    """Test serialization for a GC bucket storage object."""
    doc = GCVolume(identifier='0000', bucket_name='B0').to_dict()
    assert doc == {'type': GC_STORE, 'identifier': '0000', 'args': {'bucket': 'B0', 'prefix': None}}
    fs = GCVolume.from_dict(doc)
    assert isinstance(fs, GCVolume)
    assert fs.identifier == '0000'
    assert fs.bucket_name == 'B0'
    assert fs.prefix is None
    doc = GCVolume(identifier='0000', bucket_name='B1', prefix='dev').to_dict()
    assert doc == {'type': GC_STORE, 'identifier': '0000', 'args': {'bucket': 'B1', 'prefix': 'dev'}}
    fs = GCVolume.from_dict(doc)
    assert isinstance(fs, GCVolume)
    assert fs.identifier == '0000'
    assert fs.bucket_name == 'B1'
    assert fs.prefix == 'dev'


def test_gs_volume_subfolder(store, bucket_keys, people):
    """Test creating a new storage volume for a sub-folder of the base directory
    of a GC bucket storage volume.
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
