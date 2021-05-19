# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for methods that generate storage paths for files."""

import flowserv.model.files as files


def test_file_path_structure():
    """Test helper methods for generating file storage path values."""
    uploaddir = files.group_uploaddir('WF01', 'G01')
    assert 'WF01' in uploaddir
    assert 'G01' in uploaddir
    rundir = files.run_basedir('WF01', 'R01')
    assert 'WF01' in rundir
    assert 'R01' in rundir
    tmpdir = files.run_tmpdir()
    assert tmpdir.startswith('tmp')
    assert 'WF01' in files.workflow_basedir('WF01')
    groupdir = files.workflow_groupdir('WF01', 'G01')
    assert 'WF01' in groupdir
    assert 'G01' in groupdir
    assert 'WF01' in files.workflow_staticdir('WF01')
    assert files.workflow_staticdir('WF01') != files.workflow_basedir('WF01')
