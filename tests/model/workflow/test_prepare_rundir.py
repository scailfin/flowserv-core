# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests fot various methods that copy input and output files for workflow
runs.
"""

import os
import pytest

from flowserv.model.parameter.files import InputFile
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.serial import SerialWorkflow

import flowserv.model.template.parameter as tp
import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
WORKFLOW_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')
TEMPLATE_FILE = os.path.join(WORKFLOW_DIR, 'benchmark.yaml')
DATA_FILE = os.path.join(WORKFLOW_DIR, 'data/names.txt')


@pytest.fixture
def template():
    """Read the workflow template."""
    doc = util.read_object(TEMPLATE_FILE)
    return WorkflowTemplate.from_dict(doc, WORKFLOW_DIR, validate=True)


@pytest.mark.parametrize(
    'target_path',
    ['data/names.txt', 'data/input.txt', 'names.txt', 'input/data.dat']
)
def test_copy_default_template_files(target_path, template, tmpdir):
    """Copy the default template files and a user provided data file. Uses the
    data file in the template folder to simulate user input. Ensures that two
    code files get copied and one data file.
    """
    args = {'names': InputFile(source=DATA_FILE, target=target_path)}
    workflow = SerialWorkflow(template, args)
    files = workflow.upload_files()
    assert len(files) == 3
    util.copy_files(files, tmpdir, overwrite=False, raise_error=True)
    # Create set of expected files.
    expected_files = set({
        os.path.join(tmpdir, 'code/analyze.py'),
        os.path.join(tmpdir, 'code/helloworld.py'),
        os.path.join(tmpdir, target_path)
    })
    for dirpath, _, dirfiles in os.walk(tmpdir):
        for f in dirfiles:
            filename = os.path.join(dirpath, f)
            assert filename in expected_files
            expected_files.remove(filename)
    assert len(expected_files) == 0


@pytest.mark.parametrize(
    'target_path',
    ['data/names.txt', 'data/input.txt', 'names.txt', 'input/data.dat']
)
def test_copy_template_with_directory(target_path, template, tmpdir):
    """Modify the default template to copy the whole code directory. Uses the
    data file in the template folder to simulate user input. Ensures that three
    code files get copied and one data file.
    """
    args = {'names': InputFile(source=DATA_FILE, target=target_path)}
    template.workflow_spec['inputs']['files'] = ['code', tp.VARIABLE('names')]
    workflow = SerialWorkflow(template, args)
    files = workflow.upload_files()
    assert len(files) == 2
    util.copy_files(files, tmpdir, overwrite=False, raise_error=True)
    # Create set of expected files.
    expected_files = set({
        os.path.join(tmpdir, 'code/analyze.py'),
        os.path.join(tmpdir, 'code/helloworld.py'),
        os.path.join(tmpdir, 'code/postproc.py'),
        os.path.join(tmpdir, target_path)
    })
    for dirpath, _, dirfiles in os.walk(tmpdir):
        for f in dirfiles:
            filename = os.path.join(dirpath, f)
            assert filename in expected_files
            expected_files.remove(filename)
    assert len(expected_files) == 0


def test_error_for_existing_file(template, tmpdir):
    """Ensure an error is raised if the user provides the target path to an
    existing file.
    """
    args = {'names': InputFile(source=DATA_FILE, target='code/helloworld.py')}
    workflow = SerialWorkflow(template, args)
    files = workflow.upload_files()
    assert len(files) == 3
    with pytest.raises(ValueError):
        util.copy_files(files, tmpdir, overwrite=False, raise_error=True)
