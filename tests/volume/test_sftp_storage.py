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
from flowserv.volume.ssh import RemoteStorage, SFTP_STORE

import flowserv.util.ssh as ssh


def test_remote_volume_delete_file(mock_ssh, basedir):
    """Test downloading a file from a storage volume."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        store = RemoteStorage(remotedir=basedir, client=client)
        store.delete(key='examples/data/data.json')
    assert os.path.isdir(os.path.join(basedir, 'examples', 'data'))
    assert not os.path.isfile(os.path.join(basedir, 'examples', 'data', 'data.json'))


def test_remote_volume_copy_all(mock_ssh, basedir, emptydir, filenames_all, data_a):
    """Test copying the full directory of a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    with ssh.ssh_client('test', sep=os.sep) as client:
        target = RemoteStorage(remotedir=emptydir, client=client)
        source.copy(src=None, dst=None, store=target)
        files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all
    with files['A.json'].open() as f:
        assert json.load(f) == data_a


def test_remote_volume_copy_file(mock_ssh, basedir, emptydir, data_e):
    """Test copying a file from a storage volume."""
    source = FileSystemStorage(basedir=basedir)
    with ssh.ssh_client('test', sep=os.sep) as client:
        target = RemoteStorage(remotedir=emptydir, client=client)
        source.copy(src='examples/data/data.json', dst='static', store=target)
        files = {key: file for key, file in target.walk(src='static')}
    assert set(files.keys()) == {'static/examples/data/data.json'}
    with files['static/examples/data/data.json'].open() as f:
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


def test_remote_volume_serialization(mock_ssh, basedir):
    """Test uploading a full directory to a storage volume."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        store = RemoteStorage(remotedir=basedir, client=client)
        store_id = store.identifier
        basedir = store.remotedir
        doc = store.to_dict()
    assert doc == {
        'type': SFTP_STORE,
        'identifier': store_id,
        'args': {
            'basedir': basedir,
            'client': {
                'hostname': 'test',
                'port': None,
                'timeout': None,
                'look_for_keys': False,
                'sep': os.sep
            }
        }
    }


def test_remote_volume_subfolder(mock_ssh, basedir, data_d, data_e):
    """Test creating a new storage volume for a sub-folder of the base directory
    of a remote file system storage volume.
    """
    with ssh.ssh_client('test', sep=os.sep) as client:
        store = RemoteStorage(remotedir=basedir, client=client)
        substore = store.get_store_for_folder(key='docs', identifier='SUBSTORE')
        assert substore.identifier == 'SUBSTORE'
        with substore.load(key='D.json').open() as f:
            doc = json.load(f)
        assert doc == data_d
        substore.erase()
        with store.load(key='examples/data/data.json').open() as f:
            doc = json.load(f)
        assert doc == data_e


def test_remote_volume_upload_all(mock_ssh, basedir, emptydir, filenames_all, data_a):
    """Test uploading a full directory to a storage volume."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        source = RemoteStorage(remotedir=basedir, client=client)
        target = FileSystemStorage(basedir=emptydir)
        source.copy(src=None, dst=None, store=target)
        files = {key: file for key, file in target.walk(src='')}
    assert set(files.keys()) == filenames_all
    with files['A.json'].open() as f:
        assert json.load(f) == data_a


def test_remote_volume_upload_file(mock_ssh, basedir, emptydir, data_e):
    """Test uploading a file to a storage volume."""
    with ssh.ssh_client('test', sep=os.sep) as client:
        source = RemoteStorage(remotedir=basedir, client=client)
        target = FileSystemStorage(basedir=emptydir)
        source.copy(src='examples/data/data.json', dst='static', store=target)
        files = {key: file for key, file in target.walk(src='static')}
    assert set(files.keys()) == {'static/examples/data/data.json'}
    with files['static/examples/data/data.json'].open() as f:
        assert json.load(f) == data_e
