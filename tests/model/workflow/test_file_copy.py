# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test various methods that copy input and output files for workflow runs."""

import os
import pytest
import shutil

from robcore.io.files import FileHandle
from robcore.model.template.repo.fs import TemplateFSRepository
from robcore.model.template.parameter.value import TemplateArgument
from robcore.model.workflow.io import FileCopy

import robcore.error as err
import robcore.model.workflow.io as backend


DIR = os.path.dirname(os.path.realpath(__file__))
DATA_FILE = os.path.join(DIR, '../../.files/workflows/helloworld/data/names.txt')
INPUT_DIR = os.path.join(DIR, '../../.files/workflows/helloworld')
INPUT_FILE = os.path.join(DIR, '../../.files/schema.json')
SCRIPT_FILE = os.path.join(DIR, '../../.files/workflows/helloworld/code/script.txt')
WORKFLOW_DIR = os.path.join(DIR, '../../.files/template')

SPEC_FILE = os.path.join(WORKFLOW_DIR, 'alt-template.yaml')
SPEC_FILE_ERR = os.path.join(WORKFLOW_DIR, 'alt-upload-error.yaml')


class TestFileCopy(object):
    """Test copying files on local disk for workflow run preparation."""
    def test_input_dir_copy(self, tmpdir):
        """Test copying local directories into a workflow run directory."""
        # Copy file to target directory
        loader = FileCopy(str(tmpdir))
        loader(source=INPUT_DIR, target='workflow')
        dirname = os.path.join(str(tmpdir), 'workflow')
        assert os.path.isdir(dirname)
        assert os.path.isdir(os.path.join(dirname, 'code'))
        datadir = os.path.join(dirname, 'data')
        assert os.path.isdir(datadir)
        assert os.path.isfile(os.path.join(datadir, 'names.txt'))
        # Copy to target directory under parent that does not exist
        dst = os.path.join('run', 'files', 'wf')
        loader(source=INPUT_DIR, target=dst)
        dirname = os.path.join(str(tmpdir), dst)
        assert os.path.isdir(dirname)
        assert os.path.isdir(os.path.join(dirname, 'code'))
        datadir = os.path.join(dirname, 'data')
        assert os.path.isdir(datadir)
        assert os.path.isfile(os.path.join(datadir, 'names.txt'))

    def test_input_file_copy(self, tmpdir):
        """Test copying local input files into a workflow run directory."""
        # Copy file to target directory
        loader = FileCopy(str(tmpdir))
        loader(source=INPUT_FILE, target='input.data')
        assert os.path.isfile(os.path.join(str(tmpdir), 'input.data'))
        # Copy file to non-existing target directory
        target = os.path.join('data', 'input.data')
        loader(source=INPUT_FILE, target=target)
        assert os.path.isfile(os.path.join(str(tmpdir), target))

    def test_prepare_inputs_for_local_run(self, tmpdir):
        """Test copying input files for a local workflow run."""
        # Initialize the repository
        repo = TemplateFSRepository(base_dir=str(tmpdir))
        # Load first template
        template = repo.add_template(
            src_dir=WORKFLOW_DIR,
            spec_file=SPEC_FILE
        )
        # Create run directory
        run_dir = os.path.join(str(tmpdir), 'run')
        os.makedirs(run_dir)
        # Copy input files to run directory
        backend.upload_files(
            template=template,
            base_dir=repo.get_static_dir(template.identifier),
            files=template.workflow_spec.get('inputs', {}).get('files', []),
            arguments={
                'names': TemplateArgument(
                    template.get_parameter('names'),
                    value=FileHandle(filepath=DATA_FILE)
                )
            },
            loader=FileCopy(run_dir)
        )
        # We should have the following files in the run directory:
        # code/helloworld.py
        # data/persons.txt
        # data/friends.txt
        assert os.path.isfile(os.path.join(run_dir, 'code', 'helloworld.py'))
        assert os.path.isfile(os.path.join(run_dir, 'code', 'script.sh'))
        assert os.path.isfile(os.path.join(run_dir, 'data', 'persons.txt'))
        assert os.path.isfile(os.path.join(run_dir, 'data', 'friends.txt'))
        assert not os.path.isfile(os.path.join(run_dir, 'code', 'dontcopy.me'))
        # data/persons.txt should contain Alice and Bob
        names = set()
        with open(os.path.join(run_dir, 'data', 'persons.txt'), 'r') as f:
            for line in f:
                names.add(line.strip())
        assert len(names) == 2
        assert 'Alice' in names
        assert 'Bob' in names
        # data/friends contains Jane Doe and Joe Bloggs
        friends = set()
        with open(os.path.join(run_dir, 'data', 'friends.txt'), 'r') as f:
            for line in f:
                friends.add(line.strip())
        assert len(friends) == 2
        assert 'Jane Doe' in friends
        assert 'Joe Bloggs' in friends
        # Error cases
        with pytest.raises(err.MissingArgumentError):
            backend.upload_files(
                template=template,
                base_dir=repo.get_static_dir(template.identifier),
                files=template.workflow_spec.get('inputs', {}).get('files', []),
                arguments={},
                loader=FileCopy(run_dir)
            )
        # Error when copying non-existing file
        template = repo.add_template(
            src_dir=WORKFLOW_DIR,
            spec_file=SPEC_FILE
        )
        shutil.rmtree(run_dir)
        os.makedirs(run_dir)
        with pytest.raises(IOError):
            backend.upload_files(
                template=template,
                base_dir=repo.get_static_dir(template.identifier),
                files=template.workflow_spec.get('inputs', {}).get('files', []),
                arguments={
                    'names': TemplateArgument(
                        template.get_parameter('names'),
                        value=FileHandle(filepath=os.path.join(str(tmpdir), 'no.file'))
                    )
                },
                loader=FileCopy(run_dir)
            )
        assert not os.path.isdir(os.path.join(run_dir, 'data'))
        # If the constant value for the names parameter is removed the names
        # file is copied to the run directory and not to the data folder
        para = template.get_parameter('names')
        para.as_constant = None
        shutil.rmtree(run_dir)
        os.makedirs(run_dir)
        backend.upload_files(
            template=template,
            base_dir=repo.get_static_dir(template.identifier),
            files=template.workflow_spec.get('inputs', {}).get('files', []),
            arguments={
                'names': TemplateArgument(
                    parameter=para,
                    value=FileHandle(filepath=DATA_FILE)
                )
            },
            loader=FileCopy(run_dir)
        )
        # We should have the following files in the run directory:
        # code/helloworld.py
        # names.txt
        # data/friends.txt
        assert os.path.isfile(os.path.join(run_dir, 'code', 'helloworld.py'))
        assert os.path.isfile(os.path.join(run_dir, 'names.txt'))
        assert not os.path.isfile(os.path.join(run_dir, 'data', 'persons.txt'))
        assert os.path.isfile(os.path.join(run_dir, 'data', 'friends.txt'))
        # Template with input file parameter that is not of type file
        template = repo.add_template(
            src_dir=WORKFLOW_DIR,
            spec_file=SPEC_FILE_ERR
        )
        shutil.rmtree(run_dir)
        os.makedirs(run_dir)
        # Copy input files to run directory
        with pytest.raises(err.InvalidTemplateError):
            backend.upload_files(
                template=template,
                base_dir=repo.get_static_dir(template.identifier),
                files=template.workflow_spec.get('inputs', {}).get('files', []),
                arguments={
                    'sleeptime': TemplateArgument(
                        template.get_parameter('names'),
                        value=FileHandle(filepath=DATA_FILE)
                    )
                },
                loader=FileCopy(run_dir)
            )
