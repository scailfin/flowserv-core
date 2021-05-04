# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Fixtures for volume storage unit tests."""

import os
import pytest

import flowserv.util as util


"""Test data."""
DATA_A = {'A': 1}
DATA_B = {'B': 2}
DATA_C = {'C': 3}
DATA_D = {'D': 4}
DATA_E = [DATA_A, DATA_B]

FILE_A = 'A.json'
FILE_B = os.path.join('examples', 'B.json')
FILE_C = os.path.join('examples', 'C.json')
FILE_D = os.path.join('docs', 'D.json')
FILE_E = os.path.join('examples', 'data', 'data.json')


@pytest.fixture
def basedir(tmpdir):
    """Create the following file structure in a given base directory for unit
    tests:

      A.json                   -> DATA_A
      docs/D.json              -> DATA_D
      examples/B.json          -> DATA_B
      examples/C.json          -> DATA_C
      examples/data/data.json  -> DATA_E

    Returns the base directory containing the created files.
    """
    tmpdir = os.path.join(tmpdir, 'inputs')
    os.makedirs(tmpdir)
    # A.json
    fileA = os.path.join(tmpdir, FILE_A)
    util.write_object(obj=DATA_A, filename=fileA)
    # examples/B.json
    fileB = os.path.join(tmpdir, FILE_B)
    os.makedirs(os.path.dirname(fileB))
    util.write_object(obj=DATA_B, filename=fileB)
    # examples/C.json
    fileC = os.path.join(tmpdir, FILE_C)
    util.write_object(obj=DATA_C, filename=fileC)
    # examples/data/data.json
    fileE = os.path.join(tmpdir, FILE_E)
    os.makedirs(os.path.dirname(fileE))
    util.write_object(obj=DATA_E, filename=fileE)
    # docs/D.json
    fileD = os.path.join(tmpdir, FILE_D)
    os.makedirs(os.path.dirname(fileD))
    util.write_object(obj=DATA_D, filename=fileD)
    return tmpdir


@pytest.fixture
def data_a():
    """Content for file 'A.json'."""
    return DATA_A


@pytest.fixture
def data_e():
    """Content for file 'examples/data/data.json'."""
    return DATA_E


@pytest.fixture
def emptydir(tmpdir):
    """Get reference to an empty output directory."""
    tmpdir = os.path.join(tmpdir, 'outputs')
    os.makedirs(tmpdir)
    return tmpdir


@pytest.fixture
def filenames_all():
    """Set of names for all files in the created base directory."""
    return {'A.json', 'examples/B.json', 'examples/C.json', 'docs/D.json', 'examples/data/data.json'}
