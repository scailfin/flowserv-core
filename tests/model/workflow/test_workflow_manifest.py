# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow manifest."""

import os
import pytest

from flowserv.model.workflow.manifest import WorkflowManifest, getfile

import flowserv.error as err
import flowserv.util as util


def test_getfile_for_relative_argument(tmpdir):
    """Get user-profided file relative to the base directory."""
    filename = getfile(
        basedir=tmpdir,
        manifest_value='some.file',
        user_argument='my.file'
    )
    assert filename == os.path.join(tmpdir, 'my.file')


@pytest.mark.parametrize(
    'doc',
    [
        {'name': 'Hello World', 'workflowSpec': '.'},
        {'description': 'Hello World'},
        {'name': 'Hello World'}
    ]
)
def test_invalid_manifest(tmpdir, doc):
    """Test errors when loading invalid manifest files."""
    filename = os.path.join(tmpdir, 'flowserv.json')
    util.write_object(filename, doc)
    with pytest.raises(err.InvalidManifestError):
        WorkflowManifest.load(basedir=tmpdir, manifestfile=filename)
