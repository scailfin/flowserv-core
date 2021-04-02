# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the file system storage volume manager."""

import os

from flowserv.controller.volume.fs import FileSystemStorage
from flowserv.model.files.fs import FSFile


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '../../.files/benchmark')
HELLOWORLD_DIR = os.path.join(BENCHMARK_DIR, 'helloworld')
PREDICTOR_FILE = os.path.join(BENCHMARK_DIR, 'predictor.yaml')


def test_fs_volume_download(tmpdir):
    """Test downloading files from the storage volume."""
    env = FileSystemStorage(basedir=BENCHMARK_DIR)
    env.download(src='predictor.yaml', dst=os.path.join(tmpdir, 'workflow.yaml'))
    env.download(src='helloworld', dst=os.path.join(tmpdir, 'benchmark'))
    assert os.path.isfile(os.path.join(tmpdir, 'workflow.yaml'))
    assert os.path.isdir(os.path.join(tmpdir, 'benchmark'))
    env.close()


def test_fs_volume_init(tmpdir):
    """Test initializing the file system storage volume."""
    env = FileSystemStorage(basedir=os.path.join(tmpdir, 'env'))
    assert os.path.isdir(env.basedir)
    assert env.identifier is not None
    env.close()
    env = FileSystemStorage(basedir=os.path.join(tmpdir, 'env'), identifier='abc')
    assert os.path.isdir(env.basedir)
    assert env.identifier == 'abc'
    env.close()


def test_fs_volume_erase(tmpdir):
    """Test erasing the file system storage volume."""
    env = FileSystemStorage(basedir=os.path.join(tmpdir, 'env'))
    assert os.path.isdir(env.basedir)
    env.erase()
    assert not os.path.isdir(env.basedir)
    env.close()


def test_fs_volume_upload(tmpdir):
    """Test uploading files to the storage volume."""
    env = FileSystemStorage(basedir=os.path.join(tmpdir, 'env'))
    assert os.path.isdir(env.basedir)
    env.upload(src=PREDICTOR_FILE, dst='workflow.yaml')
    env.upload(src=FSFile(filename=PREDICTOR_FILE), dst='workflow.yaml')
    env.upload(src=HELLOWORLD_DIR, dst='benchmark')
    assert os.path.isfile(os.path.join(env.basedir, 'workflow.yaml'))
    assert os.path.isdir(os.path.join(env.basedir, 'benchmark'))
    env.close()
