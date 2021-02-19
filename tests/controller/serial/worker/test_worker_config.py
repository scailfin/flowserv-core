# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for helper methods to configure worker engines."""

import os
import sys

from flowserv.controller.serial.worker.config import java_jvm, python_interpreter


def test_get_java_jvm():
    """Test getting the path to the Java virtual machine."""
    os.environ['JAVA_HOME'] = '/usr/bin/java'
    assert java_jvm() == '/usr/bin/java/bin/java'
    del os.environ['JAVA_HOME']
    assert java_jvm() == 'java'


def test_get_python_interpreter():
    """Test getting path to Python interpreter binary."""
    assert python_interpreter() == sys.executable
