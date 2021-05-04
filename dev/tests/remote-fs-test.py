# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test uploading and downloading full directories to/from a remote file store
via sftp.
"""

import sys

from flowserv.volume.fs import FileSystemStorage
from flowserv.volume.ssh import RemoteStorage
from flowserv.util.ssh import SSHClient


def download(sourcedir, host, targetdir):
    ssh_client = SSHClient(hostname=host, port=22, look_for_keys=True)
    try:
        source = RemoteStorage(client=ssh_client, remotedir=sourcedir)
        target = FileSystemStorage(basedir=targetdir)
        source.download(src=None, store=target)
    finally:
        ssh_client.close()


def upload(sourcedir, host, targetdir):
    ssh_client = SSHClient(hostname=host, port=22, look_for_keys=True)
    try:
        source = FileSystemStorage(basedir=sourcedir)
        target = RemoteStorage(client=ssh_client, remotedir=targetdir)
        target.upload(src=None, store=source)
    finally:
        ssh_client.close()


if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) == 4 and args[0] == 'upload':
        upload(args[1], args[2], args[3])
    elif args[0] == 'download':
        download(args[1], args[2], args[3])
    else:
        print('usage: [upload | download] <dir> <host> <dir>')
        sys.exit(-1)
