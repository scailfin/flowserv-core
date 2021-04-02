# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the remote server (via SSH) storage volume manager."""

from pathlib import Path

import os

from flowserv.controller.volume.ssh import RemoteStorage
from flowserv.model.files.fs import FSFile

import flowserv.util.ssh as ssh


# Template directory
DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '../../.files/benchmark')
HELLOWORLD_DIR = os.path.join(BENCHMARK_DIR, 'helloworld')
PREDICTOR_FILE = os.path.join(BENCHMARK_DIR, 'predictor.yaml')


def test_ssh_volume_download(mock_ssh, tmpdir):
    """Test downloading files from the storage volume."""
    with ssh.ssh_client('test') as client:
        env = RemoteStorage(remotedir=BENCHMARK_DIR, client=client)
        env.download(src='predictor.yaml', dst=os.path.join(tmpdir, 'workflow.yaml'))
        env.download(src='helloworld', dst=os.path.join(tmpdir, 'benchmark'))
        assert os.path.isfile(os.path.join(tmpdir, 'workflow.yaml'))
        assert os.path.isdir(os.path.join(tmpdir, 'benchmark'))
        env.close()


def test_ssh_volume_init(mock_ssh, tmpdir):
    """Test initializing the SSH run time storage volume."""
    with ssh.ssh_client('test') as client:
        env = RemoteStorage(
            remotedir=os.path.join(tmpdir, 'env'),
            client=client
        )
        assert os.path.isdir(env.remotedir)
        assert env.identifier is not None
        env.close()
    with ssh.ssh_client('test') as client:
        env = RemoteStorage(
            remotedir=os.path.join(tmpdir, 'env'),
            client=client,
            identifier='abc'
        )
        assert os.path.isdir(env.remotedir)
        assert env.identifier == 'abc'
        env.close()


def test_ssh_volume_erase(mock_ssh, tmpdir):
    """Test erasing the remote folder."""
    remotedir = os.path.join(tmpdir, 'env')
    # -- Setup ----------------------------------------------------------------
    #
    # Create directory structure:
    # a.txt
    # b/
    # b/c.txt
    # b/d.txt
    os.makedirs(os.path.join(remotedir, 'b'))
    Path(os.path.join(remotedir, 'a.txt')).touch()
    Path(os.path.join(remotedir, 'b', 'c.txt')).touch()
    Path(os.path.join(remotedir, 'b', 'd.txt')).touch()
    with ssh.ssh_client('test') as client:
        env = RemoteStorage(
            remotedir=remotedir,
            client=client
        )
        assert os.path.isdir(remotedir)
        env.erase()
        assert not os.path.isdir(remotedir)
        env.close()


def test_ssh_volume_upload(mock_ssh, tmpdir):
    """Test uploading files to the remote storage volume."""
    remotedir = os.path.join(tmpdir, 'env')
    with ssh.ssh_client('test') as client:
        env = RemoteStorage(
            remotedir=remotedir,
            client=client
        )
        assert os.path.isdir(env.remotedir)
        env.upload(src=PREDICTOR_FILE, dst='workflow.yaml')
        env.upload(src=FSFile(filename=PREDICTOR_FILE), dst='test/workflow.yaml')
        env.upload(src=HELLOWORLD_DIR, dst='test/benchmark')
        assert os.path.isfile(os.path.join(env.remotedir, 'test', 'workflow.yaml'))
        assert os.path.isdir(os.path.join(env.remotedir, 'test', 'benchmark'))
        env.close()
