# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The run manager is used to create, delete, query, and update information
about workflow runs in an underlying database.
"""

import json
import shutil

from flowserv.core.files import InputFile
from flowserv.model.run.base import RunDescriptor, RunHandle

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.run.state as shelper
import flowserv.model.workflow.state as st


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

    def create_run(
        self, workflow_id, group_id=None, arguments=None, commit_changes=True
    ):
        """Create a new entry for a run that is in pending state. Returns a
        handle for the created run. The group identifier may be None in which
        case the run is a post-processing run that is only associated with the
        workflow but not any particular group.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        group_id: string
            Unique workflow group identifier
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
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
        # Create a unique run identifier
        run_id = util.get_unique_identifier()
        # Create an initial entry in the run table for the pending run.
        state = st.StatePending()
        sql = (
            'INSERT INTO workflow_run('
            '   run_id, workflow_id, group_id, state, created_at, arguments'
            ') VALUES(?, ?, ?, ?, ?, ?)'
        )
        # Serialize the given dictionary of workflow arguments (if not None)
        arg_values = dict()
        if arguments is not None:
            for key in arguments:
                arg_values[key] = arguments[key].value
        args_json = json.dumps(arg_values, cls=ArgumentEncoder)
        args = (
            run_id,
            workflow_id,
            group_id,
            state.type_id,
            state.created_at.isoformat(),
            args_json
        )
        self.con.execute(sql, args)
        if commit_changes:
            self.con.commit()
        # Return handle for the created run. Ensure that the run base directory
        # is created.
        rundir = self.fs.run_basedir(workflow_id, group_id, run_id)
        return RunHandle(
            identifier=run_id,
            workflow_id=workflow_id,
            group_id=group_id,
            state=state,
            arguments=json.loads(args_json),
            rundir=util.create_dir(rundir)
        )

    def delete_run(self, run_id, commit_changes=True):
        """Delete the entry for the given run from the underlying database.

        Parameters
        ----------
        run_id: string
            Unique run identifier
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
        sql = 'SELECT workflow_id, group_id FROM workflow_run WHERE run_id = ?'
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
            'SELECT workflow_id, group_id, state, arguments, '
            '   created_at, started_at, ended_at '
            'FROM workflow_run '
            'WHERE run_id = ?'
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
            workflow_id=workflow_id,
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
            arguments=json.loads(row['arguments']),
            rundir=rundir
        )

    def list_runs(self, group_id):
        """Get list of run handles for all runs that are associated with a
        given workflow group.

        Parameters
        ----------
        group_id: string, optional
            Unique workflow group identifier

        Returns
        -------
        list(flowserv.model.run.base.RunDescriptor)
        """
        runs = list()
        sql = (
            'SELECT run_id, workflow_id, state, created_at '
            'FROM workflow_run '
            'WHERE group_id = ?'
        )
        rs = self.con.execute(sql, (group_id,)).fetchall()
        for row in rs:
            run = RunDescriptor(
                identifier=row['run_id'],
                workflow_id=row['workflow_id'],
                group_id=group_id,
                state_type_id=row['state'],
                created_at=util.to_datetime(row['created_at'])
            )
            runs.append(run)
        return runs

    def poll_runs(self, group_id, state=None):
        """Get list of identifier for group runs that are currently in the
        given state. By default, the active runs are returned.

        Parameters
        ----------
        group_id: string, optional
            Unique workflow group identifier
        state: string, Optional
                State identifier query

        Returns
        -------
        list(string)
        """
        # Generate SQL query that returns the identifier of all runs in a given
        # state. If no state query is given the identifier of active runs are
        # returned.
        sql = 'SELECT run_id FROM workflow_run WHERE group_id = ? AND state'
        if state is None:
            in_clause = " IN ('{}', '{}')"
            sql = sql + in_clause.format(st.STATE_PENDING, st.STATE_RUNNING)
        else:
            sql = sql + " = '{}'".format(state.upper())
        result = list()
        for row in self.con.execute(sql, (group_id,)).fetchall():
            result.append(row[0])
        return result

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
                'file': {
                    'id': obj.identifier,
                    'name': obj.name,
                },
                'target': obj.target_path
            }
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)
