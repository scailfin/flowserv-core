# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

import boto3
import io


s3 = boto3.resource('s3')

for bucket in s3.buckets.all():
    print(bucket.name)
    for obj in bucket.objects.all():
        print('\t{}'.format(obj.key))

"""
s3 = boto3.resource('s3')
bucket = s3.Bucket('mybucket')
for obj in bucket.objects.all():
    print(obj.key, obj.last_modified)
"""

bucket = s3.Bucket('elasticbeanstalk-us-west-2-627728698163')

data = io.BytesIO()
bucket.download_fileobj('20161453fJ-vizir-db.war', data)

print(data)
