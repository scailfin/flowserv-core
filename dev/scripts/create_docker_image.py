# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""
Test for creating a Docker image from a Python base-image and a given
requirements file.
"""

import docker
import os
import shutil
import tempfile

import flowserv.util as util


tmpdir = tempfile.mkdtemp()

with open(os.path.join(tmpdir, 'requirements.txt'), 'wt') as f:
    f.write('histore\n')
    f.write('scikit-learn\n')

dockerfile = [
    'FROM python:3.9',
    'COPY requirements.txt /app/requirements.txt',
    'WORKDIR /app',
    'RUN pip install papermill',
    'RUN pip install -r requirements.txt',
    'RUN rm -Rf /app',
    'WORKDIR /'
]

with open(os.path.join(tmpdir, 'Dockerfile'), 'wt') as f:
    for line in dockerfile:
        f.write(f'{line}\n')

run_id = util.get_unique_identifier()
client = docker.from_env()
image, logs = client.images.build(path=tmpdir, tag=run_id, nocache=False)
print(image)
for line in logs:
    print(line)
client.close()

shutil.rmtree(tmpdir)
