# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of workflow resource descriptors and resource handles."""

import os
import pytest

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
