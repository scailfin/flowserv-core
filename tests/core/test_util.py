# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of Unit tests for utility methods."""

import os
import pytest

import robcore.util as util


"""JSON decode error differs between Python 2.7 and 3. This is based on:
https://www.peterbe.com/plog/jsondecodeerror-in-requests.get.json-python-2-and-3
"""
try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


class TestUtilityMethods(object):
    """Run various Unit tests form methods in the core utility module."""
    def test_datetime(self):
        """Ensure that timestamp conversion works for ISO strings with or
        without milliseconds.
        """
        for ts in ['2019-09-15T11:23:19.044133', '2019-09-15T11:23:19']:
            dt = util.to_datetime(ts)
            assert dt.year == 2019
            assert dt.month == 9
            assert dt.day == 15
            assert dt.hour == 11
            assert dt.minute == 23
            assert dt.second == 19

    def test_jquery(self):
        """Test the Json query function."""
        doc = {
            'A': 1,
            'B': 2,
            'C': {
                'D': 3,
                'E': {
                    'F': 4,
                    'G': {
                        'H': 5
                    }
                }
            },
            'I': {
                'J': 4,
                'K': [1, 2, 3]
            }
        }
        assert util.jquery(doc=doc, path=['A']) == 1
        assert util.jquery(doc=doc, path=['B']) == 2
        assert util.jquery(doc=doc, path=['C', 'D']) == 3
        assert util.jquery(doc=doc, path=['C', 'E', 'F']) == 4
        assert util.jquery(doc=doc, path=['C', 'E', 'G']) == {'H': 5}
        assert util.jquery(doc=doc, path=['I', 'K']) == [1, 2, 3]
        assert util.jquery(doc=doc, path=['C', 'E', 'G', 'H']) == 5
        assert util.jquery(doc=doc, path=['C', 'D', 'Z']) is None
        assert util.jquery(doc=doc, path=['C', 'E', 'Z']) is None
        assert util.jquery(doc=doc, path=['I', 'K', 'K']) is None
        assert util.jquery(doc=doc, path=['Z']) is None

    def test_read_write_object(self, tmpdir):
        """Test reading and writing dictionary objects to file in Json format
        and in Yaml format.
        """
        doc = {'A': 1, 'B': 2, 'C': {'D': 3}}
        json_file = os.path.join(str(tmpdir), 'file.json')
        txt_file = os.path.join(str(tmpdir), 'file.txt')
        yaml_file = os.path.join(str(tmpdir), 'file.yaml')
        # Read and write Json file
        util.write_object(filename=json_file, obj=doc)
        obj = util.read_object(filename=json_file)
        assert obj == doc
        obj = util.read_object(filename=json_file, format=util.FORMAT_JSON)
        assert obj == doc
        util.write_object(filename=json_file, obj=doc, format=util.FORMAT_YAML)
        obj = util.read_object(filename=json_file, format=util.FORMAT_YAML)
        assert obj == doc
        with pytest.raises(JSONDecodeError):
            util.read_object(filename=json_file)
        # Yaml format
        util.write_object(filename=yaml_file, obj=doc)
        obj = util.read_object(filename=yaml_file)
        assert obj == doc
        obj = util.read_object(filename=yaml_file, format=util.FORMAT_YAML)
        assert obj == doc
        util.write_object(filename=yaml_file, obj=doc, format=util.FORMAT_JSON)
        obj = util.read_object(filename=yaml_file, format=util.FORMAT_JSON)
        assert obj == doc
        # The Yaml parser can read Json files
        obj = util.read_object(filename=yaml_file)
        assert obj == doc
        # File with non-standard suffix is written in Yaml format
        util.write_object(filename=txt_file, obj=doc)
        obj = util.read_object(filename=txt_file)
        assert obj == doc
        obj = util.read_object(filename=txt_file, format=util.FORMAT_YAML)
        assert obj == doc
        with pytest.raises(JSONDecodeError):
            util.read_object(filename=txt_file, format=util.FORMAT_JSON)
        with pytest.raises(ValueError):
            util.read_object(filename=txt_file, format='UNKNOWN')
        with pytest.raises(ValueError):
            util.write_object(filename=txt_file, obj=doc, format='UNKNOWN')
