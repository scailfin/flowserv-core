# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""unit tests fot various methods that copy input and output files for workflow
runs.
"""

import os

import flowserv.util as util


DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), './.files')
INPUT_DIR = os.path.join(DIR, 'workflows/helloworld')
INPUT_FILE = os.path.join(DIR, 'schema.json')


def test_input_dir_copy(tmpdir):
    """Test copying local directories into a workflow run directory."""
    # Copy file to target directory
    files = list([(INPUT_DIR, os.path.join('workflow', 'helloworld'))])
    util.copy_files(files=files, target_dir=tmpdir)
    dirname = os.path.join(tmpdir, 'workflow', 'helloworld')
    assert os.path.isdir(dirname)
    assert os.path.isdir(os.path.join(dirname, 'code'))
    datadir = os.path.join(dirname, 'data')
    assert os.path.isdir(datadir)
    assert os.path.isfile(os.path.join(datadir, 'names.txt'))
    # Copy to target directory under parent that does not exist
    dst = os.path.join('run', 'files', 'wf')
    files = list([(INPUT_DIR, dst)])
    util.copy_files(files=files, target_dir=tmpdir)
    dirname = os.path.join(tmpdir, dst)
    assert os.path.isdir(dirname)
    assert os.path.isdir(os.path.join(dirname, 'code'))
    datadir = os.path.join(dirname, 'data')
    assert os.path.isdir(datadir)
    assert os.path.isfile(os.path.join(datadir, 'names.txt'))
    # Overwrite files.
    util.copy_files(files=files, target_dir=tmpdir)
    assert os.path.isdir(dirname)
    assert os.path.isdir(os.path.join(dirname, 'code'))


def test_input_file_copy(tmpdir):
    """Test copying local input files into a workflow run directory."""
    # Copy file to target directory
    files = list([(INPUT_FILE, 'input.data')])
    util.copy_files(files=files, target_dir=tmpdir)
    assert os.path.isfile(os.path.join(tmpdir, 'input.data'))
    # Copy file to non-existing target directory
    target = os.path.join('data', 'input.data')
    files = list([(INPUT_FILE, target)])
    util.copy_files(files=files, target_dir=tmpdir)
    assert os.path.isfile(os.path.join(tmpdir, target))
    # Overwrite files.
    util.copy_files(files=files, target_dir=tmpdir)
    assert os.path.isfile(os.path.join(tmpdir, target))
