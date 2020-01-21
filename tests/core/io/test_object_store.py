# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of the object store and the default Json store
implementation.
"""

import os
import pytest

from flowserv.core.objstore.base import ObjectStore
from flowserv.core.objstore.json import JsonFileStore

import flowserv.core.error as err
import flowserv.core.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
basedir = os.path.join(DIR, '../../.files/benchmark')
# Valid benchmark template
BENCHMARK = 'ABCDEFGH'
DEFAULT_NAME = 'benchmark.json'


class TestJsonTemplateStore(object):
    """Unit tests to read and write dictionaries from and to disk as Json
    documents.
    """
    def test_read(self):
        """Test loading benchmark templates from a valid and invalid template
        files.
        """
        # File store with default file name
        store = JsonFileStore(
            basedir=basedir,
            default_filename=DEFAULT_NAME
        )
        doc = store.read(identifier=BENCHMARK)
        columns = [c['id'] for c in doc['results']['schema']]
        assert len(columns) == 4
        for key in ['col1', 'col2', 'col3', 'col4']:
            assert key in columns
        # File store without default file name
        store = JsonFileStore(basedir=basedir)
        doc = store.read(identifier=BENCHMARK)
        columns = [c['id'] for c in doc['results']['schema']]
        assert len(columns) == 3
        for key in ['col1', 'col2', 'col3']:
            assert key in columns
        # Error when loading unknown template
        with pytest.raises(err.UnknownObjectError):
            store.read('UNKNOWN')

    def test_write(self, tmpdir):
        """Test write method of the Json store."""
        basedir = str(tmpdir)
        # Store objects as files in the base directory
        store = JsonFileStore(basedir=basedir)
        store.write(identifier=BENCHMARK, obj={'A': 1, 'B': 2})
        filename = os.path.join(basedir, BENCHMARK + '.json')
        assert os.path.isfile(filename)
        doc = store.read(identifier=BENCHMARK)
        assert doc == {'A': 1, 'B': 2}
        doc = util.read_object(filename=filename, format=util.FORMAT_JSON)
        assert doc == {'A': 1, 'B': 2}
        # Store objects as files in sub-directories of the base directory
        store = JsonFileStore(
            basedir=basedir,
            default_filename=DEFAULT_NAME
        )
        store.write(identifier=BENCHMARK, obj={'A': 1, 'C': 3})
        filename = os.path.join(basedir, BENCHMARK,  DEFAULT_NAME)
        assert os.path.isfile(filename)
        doc = store.read(identifier=BENCHMARK)
        assert doc == {'A': 1, 'C': 3}
        doc = util.read_object(filename=filename, format=util.FORMAT_JSON)
        assert doc == {'A': 1, 'C': 3}
