# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the SFTP file system IO Handle."""

import json
import os

from flowserv.volume.ssh import SFTPFile

import flowserv.util.ssh as ssh


def test_sftp_file_handle(mock_ssh, basedir, data_a):
    """Test methods of the SFTPFile handle object."""
    with ssh.ssh_client('test') as client:
        f = SFTPFile(filename=os.path.join(basedir, 'A.json'), client=client)
        with f.open() as b:
            doc = json.load(b)
        assert doc == data_a
        assert f.size() > 0
