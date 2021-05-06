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

from flowserv.tests.files import io_file
from flowserv.volume.s3 import S3File, S3Volume

import flowserv.error as err


FILES = [
    (io_file(['Alice', 'Bob']), 'data/names.txt'),
    (io_file({'a': 1}), 'code/obj1.json'),
    (io_file({'b': 2}), 'code/obj2.json')
]


@pytest.fixture
def store(mock_boto):
    volume = S3Volume(bucket_id='S3B01', identifier='V0001')
    assert volume.bucket_id == 'S3B01'
    assert volume.identifier == 'V0001'
    assert 'S3B01' in volume.describe()
    for buf, key in FILES:
        volume.store(dst=key, file=buf)
    return volume


def test_s3_erase_bucket(store):
    """Test erasing all files in a S3 bucket."""
    store.erase()
    assert len(store.bucket.bucket) == 0
    store.close()


def test_s3_load_file(store):
    """Test loading and reading a S3File handle object."""
    f = store.load(key='data/names.txt')
    with f.open() as b:
        doc = json.load(b)
    assert doc == ['Alice', 'Bob']
    assert f.size() > 0


def test_s3_query_files(store):
    """Test querying a S3 bucket store."""
    assert store.walk(src=None) == set([key for _, key in FILES])
    assert store.walk(src='code') == set({'code/obj1.json', 'code/obj2.json'})
    assert store.walk(src='code/') == set({'code/obj1.json', 'code/obj2.json'})


def test_s3_open_file(store):
    """Error case when openening an unknown file."""
    f = S3File(key='unknown', bucket=store.bucket)
    with pytest.raises(err.UnknownFileError):
        f.open()
