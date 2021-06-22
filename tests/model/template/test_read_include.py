# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for reading a workflow template that contains references to
additional files using !include (pyyaml-include).
"""

import os

import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '../../.files/benchmark/include-test')
TEMPLATE_FILE = os.path.join(BENCHMARK_DIR, 'template.yaml')


def test_read_template_with_include():
    """Test reading a template that includes other files."""
    doc = util.read_object(filename=TEMPLATE_FILE)
    assert doc['parameters'] == [
        {'name': 'names', 'label': 'Input file', 'dtype': 'file', 'target': 'data/names.txt'},
        {'name': 'sleeptime', 'label': 'Sleep time (s)', 'dtype': 'int', 'defaultValue': 10}
    ]
