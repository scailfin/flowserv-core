# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of workflow resource descriptors and resource handles."""

import os
import pytest
import tarfile

import flowserv.model.workflow.resource as wfres
import flowserv.core.util as util


def deserialize_unknown_object():
    """Unit test to ensure that an error is raised if an attempt is made to
    deserialize an dictionary with unknown or missing object type.
    """
    with pytest.raises(ValueError):
        wfres.WorkflowResource.from_dict({})
    with pytest.raises(ValueError):
        wfres.WorkflowResource.from_dict({wfres.LABEL_TYPE: 'unknown'})


def test_file_resource_handle(tmpdir):
    """Unit test for file system resource object handles."""
    filename = os.path.join(str(tmpdir), 'myfile.json')
    util.write_object(filename=filename, obj={'A': 1})
    fs = wfres.FSObject(
        identifier='ABC',
        name='results/myfile.json',
        filename=filename
    )
    assert fs.identifier == 'ABC'
    assert fs.created_at is not None
    assert fs.size > 0
    assert fs.name == 'results/myfile.json'
    assert fs.mimetype == 'application/json'
    assert os.path.isfile(fs.path)
    # Serialize and deserialize
    fh = wfres.WorkflowResource.from_dict(fs.to_dict())
    assert fh.identifier == fs.identifier
    assert fh.name == fs.name
    assert fh.size == fs.size
    assert fh.mimetype == fs.mimetype
    assert fh.created_at_local_time() == fs.created_at_local_time()


def test_resource_set():
    """Unit test for the resource set."""
    resources = wfres.ResourceSet(resources=[
        wfres.WorkflowResource(identifier='0', name='MyRes0'),
        wfres.WorkflowResource(identifier='1', name='MyRes1'),
        wfres.WorkflowResource(identifier='2', name='MyRes2')
    ])
    assert len(resources) == 3
    # Get resources by identifier
    assert resources.get_resource(identifier='0').name == 'MyRes0'
    assert resources.get_resource(identifier='1').name == 'MyRes1'
    assert resources.get_resource(identifier='2').name == 'MyRes2'
    assert resources.get_resource(identifier='3') is None
    # Get resources by name
    assert resources.get_resource(name='MyRes0').identifier == '0'
    assert resources.get_resource(name='MyRes1').identifier == '1'
    assert resources.get_resource(name='MyRes2').identifier == '2'
    assert resources.get_resource(name='MyRes3') is None
    # Error cases
    # - 1: Duplicate identifier
    with pytest.raises(ValueError):
        wfres.ResourceSet(resources=[
            wfres.WorkflowResource(identifier='0', name='MyRes0'),
            wfres.WorkflowResource(identifier='1', name='MyRes1'),
            wfres.WorkflowResource(identifier='0', name='MyRes2')
        ])
    # - 2: Duplicate name
    with pytest.raises(ValueError):
        wfres.ResourceSet(resources=[
            wfres.WorkflowResource(identifier='0', name='MyRes0'),
            wfres.WorkflowResource(identifier='1', name='MyRes0'),
            wfres.WorkflowResource(identifier='2', name='MyRes2')
        ])


def test_targzip(tmpdir):
    """Test compressing a set of file resources."""
    DIR = os.path.dirname(os.path.realpath(__file__))
    TAR_DIR = os.path.join(DIR, '../../.files/')
    resources = list()
    resources.append(
        wfres.FSObject(
            identifier='0',
            name='workflow',
            filename=os.path.join(TAR_DIR, 'benchmark')
        )
    )
    resources.append(
        wfres.FSObject(
            identifier='1',
            name='schema.json',
            filename=os.path.join(TAR_DIR, 'schema.json')
        )
    )
    out_file = os.path.join(str(tmpdir), 'run.tar.gz')
    with open(out_file, 'wb') as f:
        f.write(wfres.ResourceSet(resources).targz().getvalue())
    tar = tarfile.open(out_file, mode='r:gz')
    # Validate that there are 5 entries in the tar file
    names = list()
    for member in tar.getmembers():
        names.append(member.name)
    assert len(names) == 23
    assert 'workflow/helloworld/code/analyze.py' in names
    assert 'workflow/helloworld/data/names.txt' in names
    assert 'schema.json' in names
