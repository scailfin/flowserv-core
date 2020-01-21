# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Functions for running benchmark post-processing steps."""

import docker
import logging
import os
import shutil
import tempfile

from docker.errors import ContainerError, ImageNotFound, APIError
from string import Template

import flowserv.core.util as util


def prepare_post_proc_dir(con, ranking, files, current_run=None):
    """Create input and output directories for post-processing steps.

    The input directory contains a file submissions.json that lists the
    submissions in the leaderboard defined by the given ranking result, and as
    a sub-folder for each submission with the result files for identifier
    listed in the files argument.

    The output folder is empty.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    ranking: flowserv.model.ranking.ResultRanking
        List of runs in the current result ranking
    files: list(string)
        List of identifier for benchmark run output files that are copied into
        the input directory for each submission.
    current_run: (string, flowserv.model.workflow.StateSuccess), optional
        Identifier and state for current run that may not have been
        inserted into the database yet.

    Returns
    -------
    (string, string)
    """
    # Create temporary directories for input and output files
    in_dir = tempfile.mkdtemp()
    out_dir = tempfile.mkdtemp()
    # Create submission metadata list. The list contains an entry for each
    # result in the ranking. That entry is a dictionary with to elements: the
    # unique identifier ('id') and the submission name ('name'). For each
    # identifier a directory in the input folder is created that will contain
    # a copy of the requested input files from the run outputs.
    submissions = list()
    for run in ranking.entries:
        # Use the unque run identifier to identify a submission
        run_id  = run.run_id
        # Create directory for run fles in input directory
        run_dir = util.create_dir(os.path.join(in_dir, run_id))
        # Append submission information to the metadata list
        submissions.append({'id': run_id, 'name': run.submission_name})
    # Write submission metadata to file
    util.write_object(
        filename=os.path.join(in_dir, 'submissions.json'),
        obj=submissions
    )
    # Query the database to get the path names of all required run files.
    run_ids = ["'{}'".format(run.run_id) for run in ranking.entries]
    res_names  = ["'{}'".format(f) for f in files]
    sql = "SELECT run_id, resource_name, file_path FROM run_result_file "
    sql += "WHERE run_id IN (" + ','.join(run_ids) + ") "
    sql += "AND resource_name IN (" + ','.join(res_names) + ")"
    rs = con.execute(sql).fetchall()
    # Copy all run files to the respective folders in the input directory
    copy_files = list()
    for row in rs:
        source_file = row['file_path']
        target_file = os.path.join(in_dir, row['run_id'], row['resource_name'])
        copy_files.append((source_file, target_file))
    if current_run is not None:
        run_id, state = current_run
        for f_id in files:
            if f_id in state.files:
                source_file = state.files[f_id].file_path
                target_file = os.path.join(in_dir, run_id, f_id)
                copy_files.append((source_file, target_file))
    for source_file, target_file in copy_files:
        util.create_dir(os.path.dirname(target_file))
        shutil.copy(src=source_file, dst=target_file)
    # Return tuple of created directories
    return in_dir, out_dir


def run_post_processing(task, template_dir, in_dir, out_dir):
    """Run the post-processing task.

    Parameters
    ----------
    task: flowserv.model.template.step.PostProcessingStep
        Post-processing task descriptor
    template_dir: string
        Template source file directory
    in_dir: string
        Path to the default input directory
    out_dir: string
        Path to the default output directory
    """
    # Mount all directories that are specified in the task mount list as well
    # as the default inputs ('in') and outputs ('out') directory
    volumes = dict({
        str(os.path.abspath(in_dir)): {'bind': '/in', 'mode': 'rw'},
        str(os.path.abspath(out_dir)): {'bind': '/out', 'mode': 'rw'},
    })
    for mount_dir in task.mounts:
        source = os.path.abspath(os.path.join(template_dir, mount_dir))
        if os.path.isdir(source):
            volumes[source] = {'bind': '/{}'.format(mount_dir), 'mode': 'rw'}
    # Run the individual workflow steps using the local Docker deamon.
    client = docker.from_env()
    try:
        for cmd in task.commands:
            cmd = Template(cmd).substitute({'in': '/in', 'out': '/out'})
            client.containers.run(
                image=task.env,
                command=cmd,
                volumes=volumes
            )
    except (ContainerError, ImageNotFound, APIError) as ex:
        logging.error(ex)
