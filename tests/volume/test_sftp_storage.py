# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the remote server (via SSH) storage volume manager."""

import json
import os

from flowserv.volume.fs import FileSystemStorage
from flowserv.volume.ssh import RemoteStorage

import flowserv.util.ssh as ssh


def test_remote_volume_download_all(mock_ssh, basedir, emptydir, filenames_all, data_a):
    """Test downloading the full directory of a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    with ssh.ssh_client('test', sep=os.sep) as client:
        target = RemoteStorage(remotedir=emptydir, client=client)
        source.download(src=None, store=target)
        files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all
    with files['A.json'].open() as f:
        assert json.load(f) == data_a


def test_remote_volume_download_file(mock_ssh, basedir, emptydir, data_e):
    """Test downloading a file from a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    with ssh.ssh_client('test', sep=os.sep) as client:
        target = RemoteStorage(remotedir=emptydir, client=client)
        source.download(src='examples/data/data.json', store=target)
        files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == {'examples/data/data.json'}
    with files['examples/data/data.json'].open() as f:
        assert json.load(f) == data_e


def test_remote_volume_erase(mock_ssh, basedir):
    """Test erasing the remote storage volume base directory."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        store = RemoteStorage(remotedir=basedir, client=client)
        assert basedir in store.describe()
        store.erase()
        store.close()
    assert not os.path.isdir(basedir)


def test_remote_volume_load_file(mock_ssh, basedir, data_e):
    """Test loading a file from a remote storage volume."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        store = RemoteStorage(remotedir=basedir, client=client)
        with store.load(key='examples/data/data.json').open() as f:
            doc = json.load(f)
    assert doc == data_e


def test_remote_volume_upload_all(mock_ssh, basedir, emptydir, filenames_all, data_a):
    """Test uploading a full directory to a storage volume."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        source = RemoteStorage(remotedir=basedir, client=client)
        target = FileSystemStorage(basedir=emptydir)
        target.upload(src=None, store=source)
        files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all
    with files['A.json'].open() as f:
        assert json.load(f) == data_a


def test_remote_volume_upload_file(mock_ssh, basedir, emptydir, data_e):
    """Test uploading a file to a storage volume."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        source = RemoteStorage(remotedir=basedir, client=client)
        target = FileSystemStorage(basedir=emptydir)
        target.upload(src='examples/data/data.json', store=source)
        files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == {'examples/data/data.json'}
    with files['examples/data/data.json'].open() as f:
        assert json.load(f) == data_e