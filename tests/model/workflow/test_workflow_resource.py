# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of workflow resource descriptors and resource handles."""

import os
import pytest
import tarfile

import flowserv.model.workflow.resource as wfres


def deserialize_unknown_object():
    """Unit test to ensure that an error is raised if an attempt is made to
    deserialize an dictionary with unknown or missing object type.
    """
    with pytest.raises(ValueError):
        wfres.WorkflowResource.from_dict({})
    with pytest.raises(ValueError):
        wfres.WorkflowResource.from_dict({wfres.LABEL_TYPE: 'unknown'})


def test_resource_set():
    """Unit test for the resource set."""
    resources = wfres.ResourceSet(resources=[
        wfres.WorkflowResource(resource_id='0', key='MyRes0'),
        wfres.WorkflowResource(resource_id='1', key='MyRes1'),
        wfres.WorkflowResource(resource_id='2', key='MyRes2')
    ])
    assert len(resources) == 3
    # Get resources by identifier
    assert resources.get_resource(identifier='0').key == 'MyRes0'
    assert resources.get_resource(identifier='1').key == 'MyRes1'
    assert resources.get_resource(identifier='2').key == 'MyRes2'
    assert resources.get_resource(identifier='3') is None
    # Get resources by name
    assert resources.get_resource(key='MyRes0').resource_id == '0'
    assert resources.get_resource(key='MyRes1').resource_id == '1'
    assert resources.get_resource(key='MyRes2').resource_id == '2'
    assert resources.get_resource(key='MyRes3') is None
    # Error cases
    # - 1: Duplicate identifier
    with pytest.raises(ValueError):
        wfres.ResourceSet(resources=[
            wfres.WorkflowResource(resource_id='0', key='MyRes0'),
            wfres.WorkflowResource(resource_id='1', key='MyRes1'),
            wfres.WorkflowResource(resource_id='0', key='MyRes2')
        ])
    # - 2: Duplicate name
    with pytest.raises(ValueError):
        wfres.ResourceSet(resources=[
            wfres.WorkflowResource(resource_id='0', key='MyRes0'),
            wfres.WorkflowResource(resource_id='1', key='MyRes0'),
            wfres.WorkflowResource(resource_id='2', key='MyRes2')
        ])


def test_targzip(tmpdir):
    """Test compressing a set of file resources."""
    DIR = os.path.dirname(os.path.realpath(__file__))
    TAR_DIR = os.path.join(DIR, '../../.files/')
    resources = list()
    resources.append(
        wfres.WorkflowResource(
            resource_id='0',
            key='workflows'
        )
    )
    resources.append(
        wfres.WorkflowResource(
            resource_id='1',
            key='schema.json'
        )
    )
    out_file = os.path.join(str(tmpdir), 'run.tar.gz')
    with open(out_file, 'wb') as f:
        f.write(wfres.ResourceSet(resources).targz(TAR_DIR).getvalue())
    tar = tarfile.open(out_file, mode='r:gz')
    # Validate that there are 5 entries in the tar file
    names = list()
    for member in tar.getmembers():
        names.append(member.name)
    assert len(names) == 7
    assert 'workflows/helloworld/code/helloworld.py' in names
    assert 'workflows/helloworld/data/names.txt' in names
    assert 'schema.json' in names
