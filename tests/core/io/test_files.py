# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test input files and file handles."""

import os

from robcore.core.files import FileHandle, InputFile

import robcore.core.util as util


class TestInputFile(object):
    """Unit tests for file handles and input files."""
    def test_file_handle(self, tmpdir):
        """Test properties and methods of the file handle."""
        filename = os.path.join(str(tmpdir), 'myfile.json')
        util.write_object(filename=filename, obj={'A': 1})
        fh = FileHandle(filepath=filename)
        assert fh.created_at is not None
        assert fh.size > 0
        assert fh.name == 'myfile.json'
        assert os.path.isfile(fh.filepath)
        fh.delete()
        assert not os.path.isfile(fh.filepath)
        # Deleting a non-existing file does not raise an error
        fh.delete()

    def test_source_and_target_path(self):
        """Test source and target path methods for input file handle."""
        fh = FileHandle(filepath='/home/user/files/myfile.txt')
        # Input file handle without target path
        f = InputFile(f_handle=fh)
        assert f.source() == '/home/user/files/myfile.txt'
        assert f.target() == 'myfile.txt'
        # Input file handle with target path
        f = InputFile(f_handle=fh, target_path='data/names.txt')
        assert f.source() == '/home/user/files/myfile.txt'
        assert f.target() == 'data/names.txt'
