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
import pytest
import shutil

from flowserv.model.parameter.value import TemplateArgument
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager

import flowserv.error as err
import flowserv.util as util
import flowserv.model.template.parameter as tp


DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../.files')
DATA_FILE = os.path.join(DIR, 'workflows/helloworld/data/names.txt')
WORKFLOW_DIR = os.path.join(DIR, 'template')

specfile = os.path.join(WORKFLOW_DIR, 'alt-template.yaml')
specfile_ERR = os.path.join(WORKFLOW_DIR, 'alt-upload-error.yaml')


def test_prepare_inputs_for_local_run(database, tmpdir):
    """Test copying input files for a local workflow run."""
    # -- Setup ----------------------------------------------------------------
    fs = WorkflowFileSystem(tmpdir)
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.create_workflow(
            name='A',
            sourcedir=WORKFLOW_DIR,
            specfile=specfile
        )
        workflow_id = wf.workflow_id
    # -- Test copy input files to run directory -------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_id)
        template = wf.get_template()
        # Create run directory
        run_dir = os.path.join(tmpdir, 'run')
        os.makedirs(run_dir)
        # Copy input files to run directory
        files = tp.get_upload_files(
            template=template,
            basedir=template.sourcedir,
            files=template.workflow_spec.get('inputs', {}).get('files', []),
            arguments={
                'names': TemplateArgument(
                    template.get_parameter('names'),
                    value=DATA_FILE
                )
            }
        )
        util.copy_files(files=files, target_dir=run_dir)
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
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        manager = WorkflowManager(session=session, fs=fs)
        wf = manager.get_workflow(workflow_id)
        template = wf.get_template()
        # Missing argumen values
        with pytest.raises(err.MissingArgumentError):
            i_files = template.workflow_spec.get('inputs', {}).get('files', [])
            tp.get_upload_files(
                template=template,
                basedir=template.sourcedir,
                files=i_files,
                arguments={}
            )
        shutil.rmtree(run_dir)
        os.makedirs(run_dir)
        with pytest.raises(err.UnknownFileError):
            i_files = template.workflow_spec.get('inputs', {}).get('files', [])
            files = tp.get_upload_files(
                template=template,
                basedir=template.sourcedir,
                files=i_files,
                arguments={
                    'names': TemplateArgument(
                        template.get_parameter('names'),
                        value=os.path.join(str(tmpdir), 'no.file')
                    )
                }
            )
        assert not os.path.isdir(os.path.join(run_dir, 'data'))
        # If the constant value for the names parameter is removed an error is
        # raised.
        para = template.get_parameter('names')
        para.as_constant = None
        with pytest.raises(ValueError):
            tp.get_upload_files(
                template=template,
                basedir=template.sourcedir,
                files=template.workflow_spec.get('inputs').get('files'),
                arguments={
                    'names': TemplateArgument(
                        parameter=para,
                        value=DATA_FILE
                    )
                }
            )
