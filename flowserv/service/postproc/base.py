# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Definitions of constants for post=processing workflows."""

import os

from flowserv.model.parameter.files import File
from flowserv.model.template.parameter import ParameterIndex


"""Names for files and folders that contain run result files and run metadata.
"""
RUNS_DIR = '.runs'
RUNS_FILE = 'runs.json'


"""Labels for metadata objects in the run listing."""
LABEL_ID = 'id'
LABEL_NAME = 'name'
LABEL_FILES = 'files'


"""Fixed set of parameter declarations for post-processing workflows. Contains
only the declaration for the runs folder.
"""
PARA_RUNS = 'runs'
PARAMETER = File(
    name=PARA_RUNS,
    index=0,
    target=os.path.join(RUNS_DIR, RUNS_FILE)
)
PARAMETERS = ParameterIndex()
PARAMETERS[PARAMETER.name] = PARAMETER
