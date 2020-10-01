# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of Unit tests for utility methods."""

import io
import os
import pytest

from json.decoder import JSONDecodeError

import flowserv.util as util


def test_cleardir(tmpdir):
    """Test removong all files in a given directory."""
    # Clear an empty directory should not do anything.
    util.cleardir(tmpdir)
    # Create two file and one folder in the temp. directory.
    file_1 = os.path.join(tmpdir, 'myfile.txt')
    open(file_1, 'w').close()
    dir_1 = os.path.join(tmpdir, 'mydir')
    os.makedirs(dir_1)
    file_2 = os.path.join(dir_1, 'somefile.txt')
    open(file_2, 'w').close()
    assert os.path.isfile(file_1)
    assert os.path.isfile(file_2)
    assert os.path.isdir(dir_1)
    # Clearing the temp. directory will remove all created files and folders,
    # but the tmpdir will still exist.
    util.cleardir(tmpdir)
    assert not os.path.isfile(file_1)
    assert not os.path.isfile(file_2)
    assert not os.path.isdir(dir_1)
    assert os.path.isdir(tmpdir)


def test_datetime():
    """Ensure that timestamp conversion works for ISO strings with or
    without milliseconds.
    """
    dates = [
        '2019-09-15T11:23:19.044133',
        '2019-09-15T11:23:19',
        '20190915T11:23:19'
    ]
    for ts in dates:
        dt = util.to_datetime(ts)
        assert dt.year == 2019
        assert dt.month == 9
        assert dt.day == 15
        assert dt.hour == 11
        assert dt.minute == 23
        assert dt.second == 19


def test_jquery():
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


def test_read_write_object(tmpdir):
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
    doc = util.read_object(filename=yaml_file)
    buf = io.BytesIO(str(doc).encode("utf-8"))
    obj = util.read_object(filename=buf, format=util.FORMAT_YAML)
    assert doc == obj
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
