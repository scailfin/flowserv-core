# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory pattern for file stores."""

from typing import Dict

from flowserv.model.files.base import FileStore

import flowserv.config as config
import flowserv.error as err


def Volume(env: Dict) -> FileStore:
    """Factory pattern to create file store instances for the service API. Uses
    the environment variables FLOWSERV_FILESTORE to create an instance of the
    file store. If the environment variable is not set the FileSystemStore is
    returned as the default file store.

    The FLOWSERV_FILESTORE variable currently accepts two valid values: `fs`
    and `bucket`. For `bucket` a :class:`flowserv.model.files.bucket.BucketStore`
    is returned. The type of bucket that is use for the store is defined by
    the environment variable FLOWSERV_FILESTORE_BUCKETTYPE.

    Parameters
    ----------
    env: dict
        Configuration dictionary that provides access to configuration
        parameters from the environment.

    Returns
    -------
    flowserv.model.files.base.FileStore
    """
    class_name = env.get(config.FLOWSERV_FILESTORE, config.FILESTORE_FS)
    # If both environment variables are None return the default file store.
    # Otherwise, import the specified module and return an instance of the
    # controller class. An error is raised if only one of the two environment
    # variables is set.
    if class_name == config.FILESTORE_FS:
        from flowserv.volume.fs import FileSystemStorage
        return FileSystemStorage(env=env)
    elif class_name == config.FILESTORE_BUCKET:
        from flowserv.model.files.bucket import BucketStore
        bucket_type = env.get(config.FLOWSERV_FILESTORE_BUCKETTYPE)
        if not bucket_type:
            raise err.MissingConfigurationError('bucket type')
        if bucket_type == config.BUCKET_FS:
            from flowserv.model.files.fs import DiskBucket
            return BucketStore(bucket=DiskBucket(basedir=env.get(config.FLOWSERV_BASEDIR)))
        elif bucket_type == config.BUCKET_MEM:
            from flowserv.model.files.mem import MemBucket
            return BucketStore(bucket=MemBucket())
        elif bucket_type == config.BUCKET_S3:
            from flowserv.model.files.s3 import S3Bucket
            return BucketStore(bucket=S3Bucket(env=env))
        raise err.InvalidConfigurationError(config.FLOWSERV_FILESTORE_BUCKETTYPE, bucket_type)
    raise err.MissingConfigurationError('file store')
