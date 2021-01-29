# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module contains helper functions that prepare the input data for
post-porcessing workflows.
"""

import os
import tempfile

from typing import List, Tuple

from flowserv.model.files.base import IOHandle

import flowserv.util as util
import flowserv.service.postproc.base as base


def copy_postproc_files(
    runs: List[Tuple[str, str, List[Tuple[str, IOHandle]]]],
    outputdir: str
):
    """Copy files for runs that are included as input for a post-processing
    workflow to a given output folder.

    The list of runs contains 3-tuples of (run_id, group_name, files). The
    files element is a list of tuples of (file key, file object).

    This method also creates a metadata file in the outptu folder listing the
    included runs and run result files.

    Parameters
    ----------
    input_files: list(string)
        List of identifier for benchmark run output files that are copied into
        the input directory for each submission.
    ranking: list(flowserv.model.ranking.RunResult)
        List of runs in the current result ranking
    run_manager: flowserv.model.run.RunManager
        Manager for workflow runs
    """
    # Create the output directory if it does not exist.
    os.makedirs(outputdir, exist_ok=True)
    # Copy the given files from all workflow runs to a subfolder for each run
    # in the output directory. The output directory will also contain the
    # 'runs.json' file containing the run metadata.
    run_listing = list()
    for run_id, group_name, files in runs:
        # Create a sub-folder for the run in the output directory. Then copy
        # all given files into the created directory.
        rundir = os.path.join(outputdir, run_id)
        os.makedirs(rundir, exist_ok=True)
        for key, file in files:
            # Create target file parent directory if it does not exist.
            dst = os.path.join(rundir, key)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            # Copy run file to target file.
            file.store(dst)
        run_listing.append({
            base.LABEL_ID: run_id,
            base.LABEL_NAME: group_name,
            base.LABEL_FILES: [key for key, _ in files]
        })
    # Write the runs metadata to file
    util.write_object(
        filename=os.path.join(outputdir, base.RUNS_FILE),
        obj=run_listing
    )


def prepare_postproc_data(input_files, ranking, run_manager):
    """Create input and output directories for post-processing steps.

    The input directory contains a file runs.json that lists the runs in the
    ranking together with their group name. For each run a sub-folder with the
    run identifier as name is created. That folder contains copies of result
    files for the run for those files that are specified in the input files
    list.

    Returns the created temporary input directory.

    Parameters
    ----------
    input_files: list(string)
        List of identifier for benchmark run output files that are copied into
        the input directory for each submission.
    ranking: list(flowserv.model.ranking.RunResult)
        List of runs in the current result ranking
    run_manager: flowserv.model.run.RunManager
        Manager for workflow runs

    Returns
    -------
    string
    """
    # Create a temporary folder for the output files.
    basedir = tempfile.mkdtemp()
    # Collect information about runs and their result files.
    runs = list()
    for entry in ranking:
        run_id = entry.run_id
        group_name = entry.group_name
        files = list()
        for in_key in input_files:
            # Copy run file to target file.
            file = run_manager.get_runfile(run_id=run_id, key=in_key)
            files.append((in_key, file))
        runs.append((run_id, group_name, files))
    # Copy all collected run files to the output folder.
    copy_postproc_files(runs=runs, outputdir=basedir)
    # Return created temporary data directory
    return basedir
