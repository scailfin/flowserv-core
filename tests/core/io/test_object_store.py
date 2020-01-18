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

from robcore.core.objstore.base import ObjectStore
from robcore.core.objstore.json import JsonFileStore

import robcore.core.error as err
import robcore.core.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.join(DIR, '../../.files/benchmark')
# Valid benchmark template
BENCHMARK = 'ABCDEFGH'
DEFAULT_NAME = 'benchmark.json'


class TestAbstractObjectStore(object):
    """Unit test for abstract object store methods (included for code
    completeness).
    """
    def test_interface(self):
        """Test abstract interface methods to ensure that they raise a
        NotImplementedError.
        """
        store = ObjectStore()
        with pytest.raises(NotImplementedError):
            store.read(identifier='ABC')
        with pytest.raises(NotImplementedError):
            store.write(identifier='ABC', obj=dict())


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
            base_dir=BASE_DIR,
            default_file_name=DEFAULT_NAME
        )
        doc = store.read(identifier=BENCHMARK)
        columns = [c['id'] for c in doc['results']['schema']]
        assert len(columns) == 4
        for key in ['col1', 'col2', 'col3', 'col4']:
            assert key in columns
        # File store without default file name
        store = JsonFileStore(base_dir=BASE_DIR)
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
        base_dir = str(tmpdir)
        # Store objects as files in the base directory
        store = JsonFileStore(base_dir=base_dir)
        store.write(identifier=BENCHMARK, obj={'A': 1, 'B': 2})
        filename = os.path.join(base_dir, BENCHMARK + '.json')
        assert os.path.isfile(filename)
        doc = store.read(identifier=BENCHMARK)
        assert doc == {'A': 1, 'B': 2}
        doc = util.read_object(filename=filename, format=util.FORMAT_JSON)
        assert doc == {'A': 1, 'B': 2}
        # Store objects as files in sub-directories of the base directory
        store = JsonFileStore(
            base_dir=base_dir,
            default_file_name=DEFAULT_NAME
        )
        store.write(identifier=BENCHMARK, obj={'A': 1, 'C': 3})
        filename = os.path.join(base_dir, BENCHMARK,  DEFAULT_NAME)
        assert os.path.isfile(filename)
        doc = store.read(identifier=BENCHMARK)
        assert doc == {'A': 1, 'C': 3}
        doc = util.read_object(filename=filename, format=util.FORMAT_JSON)
        assert doc == {'A': 1, 'C': 3}
