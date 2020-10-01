# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for workflow output file specifications."""

import pytest

from flowserv.model.template.files import WorkflowOutputFile


def test_error_specification():
    """Test output file specification with errors."""
    with pytest.raises(ValueError):
        WorkflowOutputFile.from_dict(dict())
    WorkflowOutputFile.from_dict({'source': 'abc', 'unkn': 1}, validate=False)
    with pytest.raises(ValueError):
        WorkflowOutputFile.from_dict({'source': 'abc', 'unkn': 1})


def test_maximal_output_specification():
    """Test maximal spcification for a workflow output file."""
    doc = WorkflowOutputFile(
        source='myfile.txt',
        key='The file',
        title='My title',
        caption='A caption',
        widget='vega',
        format={'data': 'xyz'}
    ).to_dict()
    obj = WorkflowOutputFile.from_dict(doc)
    assert obj.source == 'myfile.txt'
    assert obj.key == 'The file'
    assert obj.title == 'My title'
    assert obj.caption == 'A caption'
    assert obj.widget == 'vega'
    assert obj.format == {'data': 'xyz'}


def test_minimal_output_specification():
    """Test minimal spcification for a workflow output file."""
    doc = WorkflowOutputFile(source='myfile.txt').to_dict()
    obj = WorkflowOutputFile.from_dict(doc)
    assert obj.source == 'myfile.txt'
    assert obj.key == obj.source
    assert obj.title is None
    assert obj.caption is None
    assert obj.widget is None
    assert obj.format is None
