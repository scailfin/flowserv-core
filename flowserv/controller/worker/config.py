# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Collection of helper methods to configure worker engines."""

import os
import sys


def python_interpreter() -> str:
    """Get path to the executable for the Python interpreter in the current
    environment.

    Returns
    -------
    string
    """
    return sys.executable


def java_jvm() -> str:
    """Get path to the Java virtual machine.

    Returns
    -------
    string
    """
    # Get path to Java Runtime (if specified in the environment variable).
    jre = os.environ.get('JAVA_HOME')
    return os.path.join(jre, 'bin', 'java') if jre else 'java'
