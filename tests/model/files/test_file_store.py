# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the bucket store. Uses the TestBucket to simulate buckets for
cloud service providers.
"""

import json
import os
import pytest

from flowserv.config import Config
from flowserv.model.files.bucket import BucketStore
from flowserv.model.files.fs import DiskBucket, FileSystemStore, FSFile, walk

import flowserv.error as err
import flowserv.util as util


"""Test data."""
DATA1 = {'A': 1}
DATA2 = {'B': 2}
DATA3 = {'C': 3}
DATA4 = {'D': 4}
EXDATA = [DATA2, DATA3]

FILE_A = 'A.json'
FILE_B = os.path.join('examples', 'B.json')
FILE_C = os.path.join('examples', 'C.json')
FILE_DATA = os.path.join('examples', 'data', 'data.json')
FILE_D = os.path.join('docs', 'D.json')


# -- Helper Methods -----------------------------------------------------------

def create_files(basedir):
    """Create file structure:
      A.json                   -> DATA1
      examples/B.json          -> DATA2
      examples/C.json          -> DATA3
      examples/data/data.json  -> EXDATA
      docs/D.json              -> DATA4

    Returns the list of file objects an their relative paths.
    """
    # Ensure that the base directory exists.
    os.makedirs(basedir, exist_ok=True)
    # Create files and keep records in file list.
    files = list()
    # A.json
    fileA = os.path.join(basedir, FILE_A)
    util.write_object(obj=DATA1, filename=fileA)
    files.append((FSFile(fileA), FILE_A))
    # examples/B.json
    fileB = os.path.join(basedir, FILE_B)
    os.makedirs(os.path.dirname(fileB))
    util.write_object(obj=DATA2, filename=fileB)
    files.append((FSFile(fileB), FILE_B))
    # examples/C.json
    fileC = os.path.join(basedir, FILE_C)
    util.write_object(obj=DATA3, filename=fileC)
    files.append((FSFile(fileC), FILE_C))
    # examples/data/data.json
    fileData = os.path.join(basedir, FILE_DATA)
    os.makedirs(os.path.dirname(fileData))
    util.write_object(obj=EXDATA, filename=fileData)
    files.append((FSFile(fileData), FILE_DATA))
    # docs/D.json
    fileD = os.path.join(basedir, FILE_D)
    os.makedirs(os.path.dirname(fileD))
    util.write_object(obj=DATA4, filename=fileD)
    files.append((FSFile(fileD), FILE_D))
    return files


def create_store(store_id, basedir):
    """Create an instance of the file store with the given identifier."""
    if store_id == 'BUCKET':
        return BucketStore(bucket=DiskBucket(basedir))
    else:
        return FileSystemStore(env=Config().basedir(basedir))


@pytest.mark.parametrize('store_id', ['FILE_SYSTEM', 'BUCKET'])
def test_delete_files_and_folders(store_id, tmpdir):
    """Test deleting folders in the file store."""
    # -- Setup ----------------------------------------------------------------
    # Initialize the file store and create files in the file store base
    # direcory.
    fs = create_store(store_id, str(tmpdir))
    create_files(str(tmpdir))
    # -- Delete folder --------------------------------------------------------
    # Initially file A, B and DATA can be read.
    assert json.load(fs.load_file(FILE_A).open()) == DATA1
    assert json.load(fs.load_file(FILE_B).open()) == DATA2
    assert json.load(fs.load_file(FILE_DATA).open()) == EXDATA
    fs.delete_folder('examples')
    # After deleting the examples folder file A can be read but not B and DATA.
    assert json.load(fs.load_file(FILE_A).open()) == DATA1
    with pytest.raises(err.UnknownFileError):
        fs.load_file(FILE_B).open()
    with pytest.raises(err.UnknownFileError):
        fs.load_file(FILE_DATA).open()
    # Delete an unknown folder has no effect.
    fs.delete_folder('examples')
    # -- Delete file ----------------------------------------------------------
    # Initially file A and D can be read.
    assert json.load(fs.load_file(FILE_A).open()) == DATA1
    assert json.load(fs.load_file(FILE_D).open()) == DATA4
    fs.delete_file(FILE_D)
    fs.delete_file(FILE_D)  # No harm deleting multiple times.
    # After deleting file D only file A but not file D can be read.
    assert json.load(fs.load_file(FILE_A).open()) == DATA1
    with pytest.raises(err.UnknownFileError):
        fs.load_file(FILE_D).open()
    # Delete an unknown file has no effect.
    fs.delete_folder(FILE_D)


@pytest.mark.parametrize('store_id', ['FILE_SYSTEM', 'BUCKET'])
def test_file_size(store_id, tmpdir):
    """Test getting the size of uploaded files."""
    # -- Setup ----------------------------------------------------------------
    # Initialize the file store and create the input file structure.
    fs = create_store(store_id, os.path.join(tmpdir, 'fs'))
    files = create_files(os.path.join(tmpdir, 'data'))
    KEY = '0000'
    fs.store_files(files=files, dst=KEY)
    # Check size of file A and DATA. File size may differ based on the system
    # but the data file should be larger in size than file A.
    size_a = fs.load_file(os.path.join(KEY, FILE_A)).size()
    assert size_a > 0
    assert fs.load_file(os.path.join(KEY, FILE_DATA)).size() > size_a


def test_file_system_walk(tmpdir):
    """Test walk function to recursively collect upload files."""
    # -- Setup ----------------------------------------------------------------
    create_files(tmpdir)
    # -- Walk that collects all files in the created file sturcture -----------
    all_files = walk(files=[(tmpdir, None)])
    all_targets = ([target for _, target in all_files])
    assert len(all_files) == 5
    assert FILE_A in all_targets
    assert FILE_B in all_targets
    assert FILE_C in all_targets
    assert FILE_D in all_targets
    assert FILE_DATA in all_targets
    # Ensure that we can load all files.
    for f, _ in all_files:
        json.load(f.open())
    # -- Walk that collects only files in the experiment folder ---------------
    result = walk(files=[(os.path.join(tmpdir, 'examples'), 'run')])
    x_files = ([target for _, target in result])
    assert len(x_files) == 3
    assert os.path.join('run', 'B.json') in x_files
    assert os.path.join('run', 'C.json') in x_files
    assert os.path.join('run', 'data', 'data.json') in x_files


@pytest.mark.parametrize('store_id', ['FILE_SYSTEM', 'BUCKET'])
def test_load_file_and_write(store_id, tmpdir):
    """Test getting a previously uploaded file and writing the content to the
    file system.
    """
    # -- Setup ----------------------------------------------------------------
    # Initialize the file store and create the input file structure. Upload
    # only file A.
    fs = create_store(store_id, os.path.join(tmpdir, 'fs'))
    files = create_files(os.path.join(tmpdir, 'data'))
    KEY = '0000'
    fs.store_files(files=[files[0]], dst=KEY)
    # -- Read and write  file A.
    filename = os.path.join(tmpdir, 'tmp')
    fs.load_file(os.path.join(KEY, FILE_A)).store(filename)
    assert util.read_object(filename) == DATA1


@pytest.mark.parametrize('store_id', ['FILE_SYSTEM', 'BUCKET'])
def test_store_and_copy_folder(store_id, tmpdir):
    """Test uploading and downloading folder files."""
    # -- Setup ----------------------------------------------------------------
    # Initialize the file store and create the input file structure.
    fs = create_store(store_id, os.path.join(tmpdir, 'fs'))
    files = create_files(os.path.join(tmpdir, 'data'))
    # -- Store all files in the file store (change file D which is the last
    # file in the returned file list to E.json instead of docs/D.json) --------
    file_d, _ = files[-1]
    files = files[:-1] + [(file_d, 'E.json')]
    KEY = '0000'
    fs.store_files(files=files, dst=KEY)
    assert json.load(fs.load_file(os.path.join(KEY, FILE_A)).open()) == DATA1
    assert json.load(fs.load_file(os.path.join(KEY, FILE_B)).open()) == DATA2
    assert json.load(fs.load_file(os.path.join(KEY, FILE_C)).open()) == DATA3
    assert json.load(fs.load_file(os.path.join(KEY, FILE_DATA)).open()) == EXDATA  # noqa: E501
    assert json.load(fs.load_file(os.path.join(KEY, 'E.json')).open()) == DATA4
    with pytest.raises(err.UnknownFileError):
        fs.load_file(os.path.join(KEY, FILE_D)).open()
    # -- Download files -------------------------------------------------------
    DOWNLOAD = os.path.join(tmpdir, 'download')
    fs.copy_folder(key=KEY, dst=DOWNLOAD)
    assert util.read_object(os.path.join(DOWNLOAD, FILE_A)) == DATA1
    assert util.read_object(os.path.join(DOWNLOAD, FILE_B)) == DATA2
    assert util.read_object(os.path.join(DOWNLOAD, FILE_C)) == DATA3
    assert util.read_object(os.path.join(DOWNLOAD, FILE_DATA)) == EXDATA
    assert util.read_object(os.path.join(DOWNLOAD, 'E.json')) == DATA4
    assert not os.path.exists(os.path.join(DOWNLOAD, FILE_D))
