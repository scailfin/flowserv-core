# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Definitions of constants for post=processing workflows. This module also
contains helper functions that prepare the input data for post-porcessing
workflows.
"""

from typing import List

from flowserv.model.files import io_file
from flowserv.model.parameter.files import File
from flowserv.model.ranking import RunResult
from flowserv.model.run import RunManager
from flowserv.model.template.parameter import ParameterIndex
from flowserv.volume.base import StorageVolume


import flowserv.util as util


"""Names for files and folders that contain run result files and run metadata.
"""
RUNS_DIR = 'runs'
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
    target=util.join(RUNS_DIR, RUNS_FILE)
)
PARAMETERS = ParameterIndex()
PARAMETERS[PARAMETER.name] = PARAMETER


# -- Helper functions ---------------------------------------------------------

def prepare_postproc_data(
    input_files: List[str], ranking: List[RunResult], run_manager: RunManager,
    store: StorageVolume
):
    """Create input files for post-processing steps for a given set of runs.

    Creates files for a post-processing run in a given base directory on a
    storage volume. The resulting directory contains files for each run in a
    given ranking. For each run a sub-folder with the run identifier as the
    directory name is created. Each folder contains copies of result files for
    the run for those files that are specified in the input files list. A file
    ``runs.json`` in the base directory lists the runs in the ranking together
    with their group name.

    Parameters
    ----------
    input_files: list(string)
        List of identifier for benchmark run output files that are copied into
        the input directory for each submission.
    ranking: list(flowserv.model.ranking.RunResult)
        List of runs in the current result ranking
    run_manager: flowserv.model.run.RunManager
        Manager for workflow runs
    store: flowserv.volume.base.StorageVolume
        Target storage volume where the created post-processing files are
        stored.
    """
    # Collect information about runs and their result files.
    runs = list()
    for entry in ranking:
        run_id = entry.run_id
        group_name = entry.group_name
        # Create a sub-folder for the run in the output directory. Then copy
        # all given files into the created directory.
        rundir = run_id
        for key in input_files:
            # Copy run file to target file.
            file = run_manager.get_runfile(run_id=run_id, key=key)
            dst = util.join(rundir, key)
            store.store(file=file, dst=dst)
        runs.append({
            LABEL_ID: run_id,
            LABEL_NAME: group_name,
            LABEL_FILES: input_files
        })
    store.store(file=io_file(runs), dst=RUNS_FILE)
