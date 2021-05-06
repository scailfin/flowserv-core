# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory pattern for file stores."""

from typing import Dict, Optional

from flowserv.config import FLOWSERV_BASEDIR
from flowserv.util.ssh import SSHClient
from flowserv.volume.base import StorageVolume
from flowserv.volume.fs import FileSystemStorage
from flowserv.volume.gc import GCVolume
from flowserv.volume.s3 import S3Volume
from flowserv.volume.ssh import RemoteStorage

import flowserv.error as err


"""Storage Volume type identifier."""
FS = 'fs'
GC = 'gc'
S3 = 's3'
SFTP = 'sftp'

"""Configuration ldictionary elements."""
ARGS = 'args'
BASEDIR = 'basedir'
BUCKET = 'bucket'
HOST = 'hostname'
KEYS = 'lookForKeys'
NAME = 'name'
PORT = 'port'
SEP = 'seperator'
TIMEOUT = 'timeout'
TYPE = 'type'


def Volume(config: Optional[Dict] = dict(), env: Optional[Dict] = dict()) -> StorageVolume:
    """Factory pattern to create file store instances for the service API.

    Expects a configuration object that contains the volume type ``type`` and
    optional volume-specific configuration arguments ``args`` and an optional
    volume identifier ``name``.

    Parameters
    ----------
    config: dict
        Configuration dictionary that provides access to configuration
        parameters for the storage volume.

    Returns
    -------
    flowserv.volume.base.StorageVolume
    """
    volume_type = config.get(TYPE, FS)
    args = config.get(ARGS, {})
    identifier = config.get(NAME)
    if volume_type == FS:
        basedir = args.get(BASEDIR, env.get(FLOWSERV_BASEDIR))
        return FileSystemStorage(basedir=basedir, identifier=identifier)
    elif volume_type == GC:
        bucket_name = args.get(BUCKET)
        if bucket_name is None:
            raise err.MissingConfigurationError('bucket identifier')
        return GCVolume(bucket_name=bucket_name, identifier=identifier)
    elif volume_type == S3:
        bucket_id = args.get(BUCKET)
        if bucket_id is None:
            raise err.MissingConfigurationError('bucket identifier')
        return S3Volume(bucket_id=bucket_id, identifier=identifier)
    elif volume_type == SFTP:
        remotedir = args.get(BASEDIR)
        if remotedir is None:
            raise err.MissingConfigurationError('remote base directory')
        hostname = args.get(HOST)
        if hostname is None:
            raise err.MissingConfigurationError('host name')
        client = SSHClient(
            hostname=hostname,
            port=args.get(PORT),
            timeout=args.get(TIMEOUT),
            look_for_keys=args.get(KEYS, False),
            sep=args.get(SEP, '/')
        )
        return RemoteStorage(client=client, remotedir=remotedir, identifier=identifier)
    raise err.InvalidConfigurationError('storage volume type', volume_type)


# -- Configuration ------------------------------------------------------------

def FStore(basedir: str, name: Optional[str] = None) -> Dict:
    """Get configuration object for Google Cloud Storage Volume.

    Parameters
    ----------
    bucket: string
        Google Cloud Storage bucket identifier.
    name: string, default=None
        Optional storage volume name.

    Returns
    -------
    dict
    """
    return VolumeConfig(type=FS, args={BASEDIR: basedir}, name=name)


def GCBucket(bucket: str, name: Optional[str] = None) -> Dict:
    """Get configuration object for Google Cloud Storage Volume.

    Parameters
    ----------
    bucket: string
        Google Cloud Storage bucket identifier.
    name: string, default=None
        Optional storage volume name.

    Returns
    -------
    dict
    """
    return VolumeConfig(type=GC, args={BUCKET: bucket}, name=name)


def S3Bucket(bucket: str, name: Optional[str] = None) -> Dict:
    """Get configuration object for AWS S3 Storage Volume.

    Parameters
    ----------
    bucket: string
        AWS S3 bucket identifier.
    name: string, default=None
        Optional storage volume name.

    Returns
    -------
    dict
    """
    return VolumeConfig(type=S3, args={BUCKET: bucket}, name=name)


def Sftp(
    remotedir: str, hostname: str, port: Optional[int] = None,
    timeout: Optional[float] = None, look_for_keys: Optional[bool] = None,
    sep: Optional[str] = None, name: Optional[str] = None
) -> Dict:
    """Get configuration object for a remote server storage volume that is
    accessed via sftp.

    Parameters
    ----------
    remotedir: string
        Base directory for stored files on the remote server.
    hostname: string
        Server to connect to.
    port: int, default=None
        Server port to connect to.
    timeout: float, default=None
        Optional timeout (in seconds) for the TCP connect.
    look_for_keys: bool, default=False
        Set to True to enable searching for discoverable private key files
        in ``~/.ssh/``.
    sep: string, default='/'
        Path separator used by the remote file system.
    name: string, default=None
        Optional storage volume name.

    Returns
    -------
    dict
    """
    args = {BASEDIR: remotedir, HOST: hostname}
    if port is not None:
        args[PORT] = port
    if timeout is not None:
        args[TIMEOUT] = timeout
    if look_for_keys is not None:
        args[KEYS] = look_for_keys
    if sep is not None:
        args[SEP] = sep
    return VolumeConfig(type=SFTP, args=args, name=name)


def VolumeConfig(type: str, args: Dict, name: Optional[str] = None) -> Dict:
    """Helper method to compose storage volume configuration parameters in a
    single dictionary.

    Parameters
    ----------
    type: string
        Storage volume type identifier.
    args: dict
        Implementation-specific volume configuration arguments.
    name: string, default=None
        Storage volume identifier.

    Returns
    -------
    dict
    """
    doc = {TYPE: type, ARGS: args}
    if name is not None:
        doc[NAME] = name
    return doc
