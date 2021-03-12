# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of bucket implementations for different cloud provider.
Expects the identifier of the provider (e.g., S3), the bucket identifier, and
a input directory. The script will upload files from the given directory,
download them, print their content, and delete them.

For testing the S3Bucket the AWS credentials have to be configured. See the
documentation for more details:
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html
"""

import sys

from flowserv.model.files.fs import walkdir
from flowserv.model.files.s3 import S3Bucket

import flowserv.config as config


if __name__ == '__main__':
    # -- Get command line parameters ------------------------------------------
    args = sys.argv[1:]
    if len(args) != 3:
        print('usage: <provider>[AWS, GC, S3] <bucket-id> <directory>')
        sys.exit(-1)
    provider_id = args[0].upper()
    bucket_id = args[1]
    indir = args[2]
    # -- Read files in the input directory ------------------------------------
    files = list()
    walkdir(src=indir, dst='template', files=files)
    # -- Create the bucket ----------------------------------------------------
    if provider_id in ['AWS', 'S3']:
        bucket = S3Bucket({config.FLOWSERV_BUCKET: bucket_id})
    else:
        raise ValueError('unknown provider {}'.format(provider_id))
    # -- Upload files ---------------------------------------------------------
    for fh, key in files:
        print('upload {}'.format(key))
        bucket.upload(file=fh, key=key)
    # -- Download files and print their content.
    keys = bucket.query('template')
    for key in keys:
        print('download {}'.format(key))
        buf = bucket.download(key)
        print(buf.read())
    # -- Delete files.
    bucket.delete(keys)
