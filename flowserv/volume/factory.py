# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory pattern for file stores."""

from typing import Dict

from flowserv.volume.base import StorageVolume
from flowserv.volume.fs import FileSystemStorage, FStore, FS_STORE  # noqa: F401
from flowserv.volume.gc import GCVolume, GCBucket, GC_STORE  # noqa: F401
from flowserv.volume.s3 import S3Volume, S3Bucket, S3_STORE  # noqa: F401
from flowserv.volume.ssh import RemoteStorage, Sftp, SFTP_STORE  # noqa: F401

import flowserv.error as err


def Volume(doc: Dict) -> StorageVolume:
    """Factory pattern to create storage volume instances for the service API.

    Expects a serialization object that contains at least the volume type ``type``.

    Parameters
    ----------
    doc: dict
        Serialization dictionary that provides access to storage volume type and
        the implementation-specific volume parameters.

    Returns
    -------
    flowserv.volume.base.StorageVolume
    """
    volume_type = doc.get('type', FS_STORE)
    if volume_type == FS_STORE:
        return FileSystemStorage.from_dict(doc)
    elif volume_type == GC_STORE:
        return GCVolume.from_dict(doc)
    elif volume_type == S3_STORE:
        return S3Volume.from_dict(doc)
    elif volume_type == SFTP_STORE:
        return RemoteStorage.from_dict(doc)
    raise err.InvalidConfigurationError('storage volume type', volume_type)
