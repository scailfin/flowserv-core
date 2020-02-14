# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Definitions of constants for post=processing workflows."""

from flowserv.model.parameter.base import TemplateParameter

import flowserv.model.parameter.declaration as pd


"""Names for files and folders that contain run result files and run metadata.
"""
RUNS_DIR = '.runs'
RUNS_FILE = 'runs.json'


"""Labels for metadata objects in the run listing."""
LABEL_ID = 'id'
LABEL_NAME = 'name'
LABEL_RESOURCES = 'resources'


"""Fixed set of parameter declarations for post-processing workflows. Contains
only the declaration for the runs folder.
"""
PARA_RUNS = 'runs'
PARAMETERS = [
    TemplateParameter(obj=pd.parameter_declaration(
        identifier=PARA_RUNS,
        data_type=pd.DT_FILE
    ))
]
