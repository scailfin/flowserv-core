# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the SSH client."""

from pathlib import Path

import os
import pytest

import flowserv.util.ssh as ssh


def test_ssh_execute_command(mock_ssh):
    """Test executing commands using the SSH client."""
    with ssh.ssh_client('test') as client:
        assert client.exec_cmd('ls') == 'ls'
    # -- Simulate error case --------------------------------------------------
    with ssh.ssh_client('test') as client:
        client._client.exit_status = -1
        with pytest.raises(RuntimeError):
            client.exec_cmd('ls')


def test_ssh_upload_download(mock_ssh, tmpdir):
    """Test file uploads and downloads via the SSH client."""
    dirpath = os.path.join(tmpdir, 'test')
    file_a = os.path.join(tmpdir, 'a.txt')
    file_b = os.path.join(dirpath, 'b.txt')
    file_c = os.path.join(tmpdir, 'c.txt')
    Path(file_a).touch()
    with ssh.ssh_client('test') as client:
        client.upload(files=[], directories=[])
        client.upload(files=[(file_a, file_b)], directories=[dirpath, dirpath])
        client.download(file_b, file_c)


def test_ssh_walk(mock_ssh, tmpdir):
    """Test the remote directory walk method."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create directory structure:
    # a.txt
    # b/
    # b/c.txt
    # b/d.txt
    # b/e/
    # b/e/f.txt
    # b/g
    os.makedirs(os.path.join(tmpdir, 'b', 'e'))
    os.makedirs(os.path.join(tmpdir, 'b', 'g'))
    Path(os.path.join(tmpdir, 'a.txt')).touch()
    Path(os.path.join(tmpdir, 'b', 'c.txt')).touch()
    Path(os.path.join(tmpdir, 'b', 'd.txt')).touch()
    Path(os.path.join(tmpdir, 'b', 'e', 'f.txt')).touch()
    Path(os.path.join(tmpdir, 'a.txt')).touch()
    # -- Test -----------------------------------------------------------------
    with ssh.ssh_client('test') as client:
        files = client.walk(tmpdir)
    assert len(files) == 4
    assert (None, 'a.txt') in files
    assert ('b', 'c.txt') in files
    assert ('b', 'd.txt') in files
    assert ('b/e', 'f.txt') in files
