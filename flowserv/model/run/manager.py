# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The run manager is used to create, delete, query, and update information
about workflow runs in an underlying database.
"""

import json
import shutil

from flowserv.core.files import InputFile
from flowserv.model.run.base import RunHandle

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.run.state as shelper
import flowserv.model.workflow.state as st


"""Labels for file handle serialization."""
LABEL_FILEHANDLE = 'fileHandle'
LABEL_FILEPATH = 'filepath'
LABEL_FILENAME = 'filename'
LABEL_ID = 'identifier'
LABEL_TARGETPATH = 'targetPath'


class RunManager(object):
    """The run manager maintains workflow runs. It provides methods the create,
    delete, and retrieve runs. the manager also provides the functionality to
    update the state of workflow runs.
    """
    def __init__(self, con, fs):
        """Initialize the connection to the underlying database and the file
        system helper to get path names for run folders.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Helper to generate file system paths to group folders
        """
        self.con = con
        self.fs = fs

    def create_run(self, workflow_id, group_id, arguments, commit_changes=True):
        """Create a new entry for a run that is in pending state. Returns a
        handle for the created run.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        group_id: string
            Unique workflow group identifier
        arguments: dict(robench.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template
        commit_changes: bool, optional
            Commit all changes to the database if true

        Returns
        -------
        flowserv.model.run.base.RunHandle

        Raises
        ------
        flowserv.core.error.MissingArgumentError
        """
        # Create a unique run identifier and a base directory for run files
        run_id = util.get_unique_identifier()
        util.create_dir(self.fs.run_basedir(workflow_id, group_id, run_id))
        # Create an initial entry in the run table for the pending run.
        state = st.StatePending()
        sql = (
            'INSERT INTO workflow_run('
            'run_id, group_id, state, created_at, arguments'
            ') VALUES(?, ?, ?, ?, ?)'
        )
        ts = state.created_at.isoformat()
        arg_values = dict()
        for key in arguments:
            arg_values[key] = arguments[key].value
        arg_serilaization = json.dumps(arg_values, cls=ArgumentEncoder)
        values = (run_id, group_id, state.type_id, ts, arg_serilaization)
        self.con.execute(sql, values)
        if commit_changes:
            self.con.commit()
        # Return handle for the created run
        return RunHandle(
            identifier=run_id,
            group_id=group_id,
            state=state,
            arguments=json.loads(arg_serilaization)
        )

    def delete_run(self, run_id, commit_changes=True):
        """Delete the entry for the given run from the underlying database.

        Parameters
        ----------
        run_id: string
            Unique submission identifier
        commit_changes: bool, optional
            Commit all changes to the database if true

        Raises
        ------
        flowserv.core.error.UnknownRunError
        """
        # Retrieve the workflow identifier and group identifier from the
        # database in order to be able to generate the path for the run folder.
        # If the result is empty we assume that the run identifier is unknown
        # and raise an error.
        sql = (
            'SELECT g.workflow_id, g.group_id '
            'FROM workflow_group g, workflow_run r '
            'WHERE g.group_id = r.group_id AND r.run_id = ?'
        )
        row = self.con.execute(sql, (run_id,)).fetchone()
        if row is None:
            raise err.UnknownRunError(run_id)
        # Get base directory for run files
        workflow_id = row['workflow_id']
        group_id = row['group_id']
        rundir = self.fs.run_basedir(workflow_id, group_id, run_id)
        # Create list of SQL statements to delete all records that are
        # associated with the workflow run from the underlying database.
        stmts = list()
        stmts.append('DELETE FROM run_result_file WHERE run_id = ?')
        stmts.append('DELETE FROM run_error_log WHERE run_id = ?')
        stmts.append('DELETE FROM workflow_run WHERE run_id = ?')
        for sql in stmts:
            self.con.execute(sql, (run_id,))
        # Commit changes only of the respective flag is True
        if commit_changes:
            self.con.commit()
        # Delete the base directory containing group files
        shutil.rmtree(rundir)

    def fetch_row(self, run_id):
        """Get database row object that contains all the basic information for
        the given run. Raises an error if the run is unknown.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        dict()

        Raises
        ------
        flowserv.core.error.UnknownRunError
        """
        # Fetch run information from the database. If the result is None the
        # run is unknown and an error is raised.
        sql = (
            'SELECT r.run_id, g.workflow_id, g.group_id, r.state, r.arguments, '
            'r.created_at, r.started_at, r.ended_at '
            'FROM workflow_group g, workflow_run r '
            'WHERE g.group_id = r.group_id AND r.run_id = ?'
        )
        row = self.con.execute(sql, (run_id,)).fetchone()
        if row is None:
            raise err.UnknownRunError(run_id)
        return row

    def get_run(self, run_id):
        """Get handle for the given run from the underlying database.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.model.run.base.RunHandle

        Raises
        ------
        flowserv.core.error.UnknownRunError
        """
        # Fetch run information from the database. Raises an error if the run
        # is unknown..
        row = self.fetch_row(run_id)
        # Get base directory for run files
        workflow_id = row['workflow_id']
        group_id = row['group_id']
        rundir = self.fs.run_basedir(workflow_id, group_id, run_id)
        # Create and return run handle. Use the loader helper methods to
        # retrieve the current trun state
        return RunHandle(
            identifier=run_id,
            group_id=group_id,
            state=shelper.get_run_state(
                con=self.con,
                run_id=run_id,
                state_id=row['state'],
                created_at=util.to_datetime(row['created_at']),
                started_at=util.to_datetime(row['started_at']),
                ended_at=util.to_datetime(row['ended_at']),
                basedir=rundir
            ),
            arguments=json.loads(row['arguments'])
        )

    def update_run(self, run_id, state, commit_changes=True):
        """Update the state of the given run. This method does check if the
        state transition is valid. Transitions are valid for active workflows,
        if the transition is (a) from pending to running or (b) to an inactive
        state. Invalid state transitions will raise an error.

        Parameters
        ----------
        run_id: string
            Unique identifier for the run
        state: flowserv.model.workflow.state.WorkflowState
            New workflow state
        commit_changes: bool, optional
            Commit all changes to the database if true

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownRunError
        """
        # Fetch run information from the database. Raises an error if the run
        # is unknown..
        row = self.fetch_row(run_id)
        # Get current state identifier to check whether the state transition is
        # valid.
        current_state = row['state']
        # Only update the state in the database if the workflow is not pending.
        # For pending workflows an entry is created when the run starts.
        # -- RUNNING ----------------------------------------------------------
        if state.is_running():
            if current_state != st.STATE_PENDING:
                msg = 'cannot start run in {} state'
                raise err.ConstraintViolationError(msg.format(current_state))
            shelper.set_running(
                con=self.con,
                run_id=run_id,
                started_at=state.started_at
            )
        # -- CANCELED ---------------------------------------------------------
        elif state.is_canceled():
            if current_state not in st.ACTIVE_STATES:
                msg = 'cannot cancel run in {} state'
                raise err.ConstraintViolationError(msg.format(current_state))
            shelper.set_error(
                con=self.con,
                run_id=run_id,
                started_at=state.started_at,
                stopped_at=state.stopped_at,
                messages=state.messages,
                is_canceled=True
            )
        # -- ERROR ------------------------------------------------------------
        elif state.is_error():
            if current_state not in st.ACTIVE_STATES:
                msg = 'cannot set run in {} state to error state'
                raise err.ConstraintViolationError(msg.format(current_state))
            shelper.set_error(
                con=self.con,
                run_id=run_id,
                started_at=state.started_at,
                stopped_at=state.stopped_at,
                messages=state.messages
            )
        # -- SUCCESS ----------------------------------------------------------
        elif state.is_success():
            if current_state not in st.ACTIVE_STATES:
                msg = 'cannot set run in {} state to error state'
                raise err.ConstraintViolationError(msg.format(current_state))
            shelper.set_success(
                con=self.con,
                run_id=run_id,
                started_at=state.started_at,
                finished_at=state.finished_at,
                resources=state.resources
            )
        # -- PENDING ----------------------------------------------------------
        elif current_state != st.STATE_PENDING:
            msg = 'cannot set run in pending state to {}'
            raise err.ConstraintViolationError(msg.format(state.type_id))

        # Commit changes if flag is True
        if commit_changes:
            self.con.commit()


# -- Helper classes -----------------------------------------------------------

class ArgumentEncoder(json.JSONEncoder):
    """JSON encoder for run argument values. The encoder is required to handle
    argument values that are input file objects.
    """
    def default(self, obj):
        if isinstance(obj, InputFile):
            return {
                LABEL_FILEHANDLE: {
                    LABEL_FILEPATH: obj.filename,
                    LABEL_ID: obj.identifier,
                    LABEL_FILENAME: obj.name,
                },
                LABEL_TARGETPATH: obj.target_path
            }
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)
