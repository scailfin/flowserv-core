# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the S3 bucket store. Uses the TestBucket to simulate S3
buckets.
"""

import json
import os
import pytest
import shutil
import tarfile

from flowserv.model.files.s3 import BucketStore
from flowserv.tests.files import FakeStream, MemBucket

import flowserv.error as err
import flowserv.util as util


"""Test data."""
DATA1 = {'A': 1}
DATA2 = {'B': 2}
DATA3 = {'C': 3}
DATA4 = {'D': 4}
EXDATA = [DATA2, DATA3]


# -- Helper Methods -----------------------------------------------------------

def create_files(basedir):
    """Create file structure:
      A.json
      examples/B.json
      examples/C.json
      docs/D.json
    """
    fileA = os.path.join(basedir, 'A.json')
    FakeStream(data=DATA1).write(fileA)
    f_examples = os.path.join(basedir, 'examples')
    os.makedirs(f_examples)
    fileB = os.path.join(f_examples, 'B.json')
    FakeStream(data=DATA2).write(fileB)
    fileC = os.path.join(f_examples, 'C.json')
    FakeStream(data=DATA3).write(fileC)
    f_data = os.path.join(f_examples, 'data')
    os.makedirs(f_data)
    fileData = os.path.join(f_data, 'data.json')
    FakeStream(data=EXDATA).write(fileData)
    f_docs = os.path.join(basedir, 'docs')
    os.makedirs(f_docs)
    fileD = os.path.join(f_docs, 'D.json')
    FakeStream(data=DATA4).write(fileD)


def test_delete_files(tmpdir):
    """Test deleting files in the file store."""
    # -- Setup ----------------------------------------------------------------
    # Initialize bucket store.
    fs = BucketStore(MemBucket())
    # Create file structure:
    create_files(tmpdir)
    # -- Copy files -----------------------------------------------------------
    files = [
        ('A.json', 'A.json'),
        ('examples', 'examples'),
        ('A.json', 'examples.json')
    ]
    fs.copy_files(src=tmpdir, files=files)
    # -- Delete files ---------------------------------------------------------
    assert json.load(fs.load_file('A.json')) == DATA1
    fs.delete_file('A.json')
    with pytest.raises(err.UnknownFileError):
        fs.load_file('A.json')
    assert json.load(fs.load_file('examples/B.json')) == DATA2
    fs.delete_file('examples')
    with pytest.raises(err.UnknownFileError):
        fs.load_file('examples/B.json')
    assert json.load(fs.load_file('examples.json')) == DATA1
    # Unknown file has no effect.
    fs.delete_file('examples/B.json')


def test_download_archive(tmpdir):
    """Test downloading files and directories as tar archive."""
    # -- Setup ----------------------------------------------------------------
    # Initialize bucket store.
    fs = BucketStore(MemBucket())
    # Create file structure:
    create_files(tmpdir)
    # -- Copy files -----------------------------------------------------------
    files = [
        ('A.json', 'A.json'),
        ('examples', 'examples'),
        ('docs', 'examples/docs')
    ]
    fs.copy_files(src=tmpdir, files=files)
    # -- Download archive -----------------------------------------------------
    files = [
        ('B.json', 'B.json'),
        ('C.json', 'A.json'),
        ('docs/D.json', ('docs/D.json'))
    ]
    archive = fs.download_archive(src='examples', files=files)
    tar = tarfile.open(fileobj=archive, mode='r:gz')
    members = [t.name for t in tar.getmembers()]
    assert len(members) == 3
    assert 'A.json' in members
    assert 'B.json' in members
    assert 'docs/D.json' in members


def test_file_copy_and_download(tmpdir):
    """Test uploading and downloading lists of files and directories."""
    # -- Setup ----------------------------------------------------------------
    # Initialize bucket store.
    fs = BucketStore(MemBucket())
    # Create file structure:
    create_files(tmpdir)
    # -- Copy files -----------------------------------------------------------
    files = [
        ('A.json', 'A.json'),
        ('A.json', 'B.json'),
        ('B.json', 'C.json'),
        ('examples/', 'examples'),
        ('examples', 'notes/'),
        ('docs', 'notes')
    ]
    fs.copy_files(src=tmpdir, files=files)
    # -- Load files -----------------------------------------------------------
    assert json.load(fs.load_file('A.json')) == DATA1
    assert json.load(fs.load_file('B.json')) == DATA1
    assert json.load(fs.load_file('examples/B.json')) == DATA2
    assert json.load(fs.load_file('notes/B.json')) == DATA2
    assert json.load(fs.load_file('notes/D.json')) == DATA4
    with pytest.raises(err.UnknownFileError):
        fs.load_file('C.json')
    # -- Download files -------------------------------------------------------
    f_download = os.path.join(tmpdir, 'downloads')
    files = [('B.json', 'results/A.json'), ('notes/', 'results')]
    fs.download_files(files=files, dst=f_download)
    f_results = os.path.join(f_download, 'results')
    assert util.read_object(os.path.join(f_results, 'A.json')) == DATA1
    assert util.read_object(os.path.join(f_results, 'B.json')) == DATA2
    assert util.read_object(os.path.join(f_results, 'C.json')) == DATA3
    assert util.read_object(os.path.join(f_results, 'D.json')) == DATA4
    shutil.rmtree(f_results)
    files = [
        ('notes', 'results/'),
        ('examples/data/data.json', 'results/E.json')
    ]
    fs.download_files(files=files, dst=f_download)
    f_results = os.path.join(f_download, 'results')
    assert util.read_object(os.path.join(f_results, 'B.json')) == DATA2
    assert util.read_object(os.path.join(f_results, 'C.json')) == DATA3
    assert util.read_object(os.path.join(f_results, 'D.json')) == DATA4
    assert util.read_object(os.path.join(f_results, 'E.json')) == EXDATA


def test_file_upload_and_load(tmpdir):
    """Test uploading and downloading files and and file objects."""
    # -- Setup ----------------------------------------------------------------
    # Initialize bucket store.
    fs = BucketStore(MemBucket())
    # Test files.
    file1 = os.path.join(tmpdir, 'A.json')
    FakeStream(data=DATA1).write(file1)
    file2 = FakeStream(data=DATA2).save()
    # -- Upload files ---------------------------------------------------------
    fs.upload_file(file=file1, dst='files/A.json')
    fs.upload_file(file=file2, dst='B.json')
    # -- Load files -------------------------------------------------------
    buf = fs.load_file('files/A.json')
    assert json.load(buf) == DATA1
    buf = fs.load_file('B.json')
    assert json.load(buf) == DATA2
    # -- Error cases ----------------------------------------------------------
    with pytest.raises(err.UnknownFileError):
        fs.load_file('unknown')
