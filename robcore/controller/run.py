# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The run store is a collection of methods that create, delete, query, and
update information about workflow runs in an underlying database. These methods
are intended for use by other components of the reproducible open benchmark
platform, e.g., the submission manager and the workflow execution engine.
"""

import json
import os
import shutil

from robcore.io.files import InputFile
from robcore.model.resource import FileResource
from robcore.model.template.command import PostProcessingStep
from robcore.model.template.schema import ResultSchema
from robcore.model.workflow.run import RunHandle

import robcore.controller.postproc as postproc
import robcore.error as err
import robcore.model.ranking as ranking
import robcore.model.workflow.state as st
import robcore.util as util


"""Labels for file handle serialization."""
LABEL_FILEHANDLE = 'fileHandle'
LABEL_FILEPATH = 'filepath'
LABEL_FILENAME = 'filename'
LABEL_ID = 'identifier'
LABEL_TARGETPATH = 'targetPath'


# -- Methods for maintaining run information in the database ------------------

def create_run(con, submission_id, arguments, commit_changes=True):
    """Create a new entry for a run that is in pending state. Returns a handle
    for the created run.

    This method does not commit changes to the underlying database if the
    respective flag is False, i.e., the method is called from within another
    method that needs to commit (or roll back) additional changes.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    submission_id: string
        Unique submission identifier
    arguments: dict(robench.model.template.parameter.value.TemplateArgument)
        Dictionary of argument values for parameters in the template
    commit_changes: bool, optional
        Commit all changes to the database if true

    Returns
    -------
    robcore.model.workflow.run.RunHandle

    Raises
    ------
    robcore.error.MissingArgumentError
    """
    # Create a unique run identifier
    run_id = util.get_unique_identifier()
    # Create an initial entry in the run table for the pending run.
    state = st.StatePending()
    sql = 'INSERT INTO benchmark_run('
    sql += 'run_id, submission_id, state, created_at, arguments'
    sql += ') VALUES(?, ?, ?, ?, ?)'
    ts = state.created_at.isoformat()
    arg_values = dict()
    for key in arguments:
        arg_values[key] = arguments[key].value
    arg_serilaization = json.dumps(arg_values, cls=ArgumentEncoder)
    values = (run_id, submission_id, state.type_id, ts, arg_serilaization)
    con.execute(sql, values)
    if commit_changes:
        con.commit()
    # Return handle for the created run
    return RunHandle(
        identifier=run_id,
        submission_id=submission_id,
        state=state,
        arguments=json.loads(arg_serilaization)
    )


def delete_run(con, run_id, commit_changes=True):
    """Delete the entry for the given run from the underlying database. This
    will not remove any result file resources that are stored on the file
    system.

    The method does not check the run state before deletion. That is, deleting
    a run that is in an active state will not raise an error.

    This method does not commit changes to the underlying database if the
    respective flag is False, i.e., the method is called from within another
    method that needs to commit (or roll back) additional changes.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    run_id: string
        Unique submission identifier
    commit_changes: bool, optional
        Commit all changes to the database if true
    """
    # Delete all rows in the database that belong to the run.
    sql = 'DELETE FROM {} WHERE run_id = ?'
    for table_name in ['run_result_file', 'run_error_log', 'benchmark_run']:
        con.execute(sql.format(table_name), (run_id,))
    if commit_changes:
        con.commit()


def exists_run(con, run_id):
    """Test if a run with the given identifier exists in the underlying
    database.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    run_id: string
        Unique run identifier

    Returns
    -------
    bool
    """
    sql = 'SELECT run_id FROM benchmark_run WHERE run_id = ?'
    return not con.execute(sql, (run_id,)).fetchone() is None


def get_run(con, run_id):
    """Get handle for the given run from the underlying database.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    run_id: string
        Unique run identifier

    Returns
    -------
    robcore.model.workflow.run.RunHandle

    Raises
    ------
    robcore.error.UnknownRunError
    """
    # Fetch run information from the database. If the result is None the
    # run is unknown and an error is raised.
    sql = 'SELECT r.run_id, s.submission_id, r.state, '
    sql += 'r.arguments, r.created_at, r.started_at, r.ended_at '
    sql += 'FROM benchmark_submission s, benchmark_run r '
    sql += 'WHERE s.submission_id = r.submission_id AND r.run_id = ?'
    row = con.execute(sql, (run_id,)).fetchone()
    if row is None:
        raise err.UnknownRunError(run_id)
    run_id = row['run_id']
    submission_id = row['submission_id']
    type_id = row['state']
    created_at = util.to_datetime(row['created_at'])
    started_at = util.to_datetime(row['started_at'])
    ended_at = util.to_datetime(row['ended_at'])
    arguments = row['arguments']
    if type_id == st.STATE_PENDING:
        state = st.StatePending(created_at=created_at)
    elif type_id == st.STATE_RUNNING:
        state = st.StateRunning(
            created_at=created_at,
            started_at=started_at
        )
    elif type_id in [st.STATE_CANCELED, st.STATE_ERROR]:
        # Read error messages from the database
        messages = list()
        sql = 'SELECT * FROM run_error_log WHERE run_id = ? ORDER BY pos'
        for msg in con.execute(sql, (run_id,)).fetchall():
            messages.append(msg['message'])
        if type_id == st.STATE_CANCELED:
            state = st.StateCanceled(
                created_at=created_at,
                started_at=started_at,
                stopped_at=ended_at,
                messages=messages
            )
        else:
            state = st.StateError(
                created_at=created_at,
                started_at=started_at,
                stopped_at=ended_at,
                messages=messages
            )
    else: # type_id == state.STATE_SUCCESS:
        # Read file resources that were generated by the run
        files = dict()
        sql = 'SELECT * FROM run_result_file WHERE run_id = ?'
        for f in con.execute(sql, (run_id,)).fetchall():
            resource_name = f['resource_name']
            files[resource_name] = FileResource(
                resource_id=f['file_id'],
                resource_name=resource_name,
                file_path=f['file_path']
            )
        state = st.StateSuccess(
            created_at=created_at,
            started_at=started_at,
            finished_at=ended_at,
            files=files
        )
    # Create and return run handle
    return RunHandle(
        identifier=run_id,
        submission_id=submission_id,
        state=state,
        arguments=json.loads(arguments)
    )


def update_run(con, run_id, state, commit_changes=True):
    """Update the state of the given run. This method does check if the
    state transition is valid. Transitions are valid for active workflows,
    if the transition is (a) from pending to running or (b) to an inactive
    state. Invalid state transitions will raise an error.

    This method does not commit changes to the underlying database if the
    respective flag is False, i.e., the method is called from within another
    method that needs to commit (or roll back) additional changes.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    run_id: string
        Unique identifier for the run
    state: robcore.model.workflow.state.WorkflowState
        New workflow state
    commit_changes: bool, optional
        Commit all changes to the database if true

    Raises
    ------
    robcore.error.ConstraintViolationError
    """
    # Query template to update the state.
    sqltmpl = (
        "UPDATE benchmark_run SET state='" + state.type_id + "', {} "
        "WHERE run_id = '" + run_id + "'"
    )
    stmts = list()
    # Only update the state in the database if the workflow is not pending.
    # For pending workflows an entry is created when the run starts.
    # -- RUNNING --------------------------------------------------------------
    if state.is_running():
        stmts.append(
            (sqltmpl.format('started_at=?'),
            (state.started_at.isoformat(),))
        )
    # -- CANCELED -------------------------------------------------------------
    elif state.is_canceled():
        stmts.append(
            (sqltmpl.format('started_at=?, ended_at=?'),
            (state.started_at.isoformat(), state.stopped_at.isoformat()))
        )
        # Insert statements for error messages
        instmpl = (
            'INSERT INTO run_error_log(run_id, message, pos) '
            'VALUES(?, ?, ?)'
        )
        messages = state.messages
        for i in range(len(messages)):
            stmts.append((instmpl, (run_id, messages[i], i)))
    # -- ERROR ----------------------------------------------------------------
    elif state.is_error():
        stmts.append(
            (sqltmpl.format('started_at=?, ended_at=?'),
            (state.started_at.isoformat(), state.stopped_at.isoformat()))
        )
        # Insert statements for error messages
        instmpl = (
            'INSERT INTO run_error_log(run_id, message, pos) '
            'VALUES(?, ?, ?)'
        )
        messages = state.messages
        for i in range(len(messages)):
            stmts.append((instmpl, (run_id, messages[i], i)))
    # -- SUCCESS --------------------------------------------------------------
    elif state.is_success():
        stmts.append(
            (sqltmpl.format('started_at=?, ended_at=?'),
            (state.started_at.isoformat(), state.finished_at.isoformat()))
        )
        # SQL statements to insert run result file information
        instmpl = (
            'INSERT INTO run_result_file'
            '(run_id, file_id, resource_name, file_path) '
            'VALUES(?, ?, ?, ?)'
        )
        for f in state.files.values():
            insargs = (run_id, f.resource_id, f.resource_name, f.file_path)
            stmts.append((instmpl, insargs))
        # Insert the result values.
        ranking.insert_run_results(
            con=con,
            run_id=run_id,
            files=state.files,
            commit_changes=False
        )
        # Get the identifier of the associated benchmark together with (i) the
        # definitions of the result schema, (ii) the optional post-processing
        # step, (iii) the key for the most recent post-processing results, and
        # (iv) the template folders for static files and post-processing
        # results.
        sql = (
            'SELECT b.benchmark_id, b.result_schema, b.postproc_task, '
            'b.resources_key, b.static_dir, b.resources_dir '
            'FROM benchmark b, benchmark_submission s, benchmark_run r '
            'WHERE b.benchmark_id = s.benchmark_id AND '
            's.submission_id = r.submission_id AND r.run_id = ?'
        )
        rs = con.execute(sql, (run_id,)).fetchone()
        if rs['result_schema'] is not None and rs['postproc_task'] is not None:
            # Get the latest leader board for the benchmark to see if it it has
            # changes since the last post-processing execution.
            benchmark_id = rs['benchmark_id']
            leaders = ranking.query(
                con=con,
                benchmark_id=benchmark_id,
                schema=ResultSchema.from_dict(json.loads(rs['result_schema']))
            )
            # Use sorted list of run identifier in the ranking as unique key
            # to identify changes to previous post-processing results.
            runs = sorted([r.run_id for r in leaders.entries])
            resource_key = rs['resources_key']
            if resource_key is not None:
                resource_key = json.loads(resource_key)
            else:
                resource_key = list()
            if runs != resource_key:
                # Run post-processing task if the current post-processing
                # resources where generated for a different set of runs than
                # those in the current leader board.
                # Get post-processing task definition
                doc = json.loads(rs['postproc_task'])
                postproc_task = PostProcessingStep.from_dict(doc)
                # Prepare the submission data and output directory
                in_dir, out_dir = postproc.prepare_post_proc_dir(
                    con=con,
                    ranking=leaders,
                    files=postproc_task.inputs,
                    current_run=(run_id, state)
                )
                # Run workflow step in a Docker container
                postproc.run_post_processing(
                    task=postproc_task,
                    template_dir=rs['static_dir'],
                    in_dir=in_dir,
                    out_dir=out_dir
                )
                # Copy files from output directory to the benchmark's resource
                # directory. Create a new unique subfolder for all resources.
                res_run_id = util.get_unique_identifier()
                resources_dir = util.create_dir(
                    os.path.join(rs['resources_dir'], res_run_id)
                )
                for filename in postproc_task.outputs:
                    source_file = os.path.join(out_dir, filename)
                    target_file = os.path.join(resources_dir, filename)
                    if os.path.isfile(source_file):
                        shutil.copy(src=source_file, dst=target_file)
                # Update resource key in the benchmark table
                sql = (
                    "UPDATE benchmark "
                    "SET resource_key = ?, resources_id = ? "
                    "WHERE benchmark_id = ?"
                )
                con.execute(sql, (json.dumps(runs), res_run_id, benchmark_id))
                # Delete temporary input and output directories
                shutil.rmtree(in_dir)
                shutil.rmtree(out_dir)
    # Execute all SQL statements
    for sql, values in stmts:
        con.execute(sql, values)
    if commit_changes:
        con.commit()


# -- Helper classes -----------------------------------------------------------

class ArgumentEncoder(json.JSONEncoder):
    """JSON encoder for run argument values. The encoder is required to handle
    argument values that are input file objects.
    """
    def default(self, obj):
        if isinstance(obj, InputFile):
            return {
                LABEL_FILEHANDLE: {
                    LABEL_FILEPATH: obj.filepath,
                    LABEL_ID: obj.identifier,
                    LABEL_FILENAME: obj.file_name,
                },
                LABEL_TARGETPATH: obj.target_path
            }
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)
