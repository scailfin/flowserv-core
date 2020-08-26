# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The run manager is used to create, delete, query, and update information
about workflow runs in an underlying database.
"""

import os
import shutil

from flowserv.model.base import RunFile, RunHandle, RunMessage

import flowserv.error as err
import flowserv.model.workflow.state as st
import flowserv.util as util


class RunManager(object):
    """The run manager maintains workflow runs. It provides methods the create,
    delete, and retrieve runs. the manager also provides the functionality to
    update the state of workflow runs.
    """
    def __init__(self, session, fs):
        """Initialize the connection to the underlying database and the file
        system helper to get path names for run folders.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Helper to generate file system paths to group folders
        """
        self.session = session
        self.fs = fs

    def create_run(self, workflow=None, group=None, arguments=None, runs=None):
        """Create a new entry for a run that is in pending state. Returns a
        handle for the created run.

        A run is either created for a group (i.e., a grop submission run) or
        for a workflow (i.e., a post-processing run). Only one of the two
        parameters is expected to be None.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowHandle, default=None
            Workflow handle if this is a post-processing run.
        group: flowserv.model.base.GroupHandle
            Group handle if this is a group sumbission run.
        arguments: list
            List of argument values for parameters in the template.
        runs: list(string), default=None
            List of run identifier that define the input for a post-processing
            run.

        Returns
        -------
        flowserv.model.base.RunHandle

        Raises
        ------
        ValueError
        flowserv.error.MissingArgumentError
        """
        # Ensure that only group or workflow is given.
        if workflow is None and group is None:
            raise ValueError('missing arguments for workflow or group')
        elif workflow is not None and group is not None:
            raise ValueError('arguments for workflow or group')
        elif group is not None and runs is not None:
            raise ValueError('unexpected argument runs')
        # Create a unique run identifier.
        run_id = util.get_unique_identifier()
        # Get workflow and group identifier.
        if workflow is None:
            workflow_id = group.workflow_id
            group_id = group.group_id
        else:
            workflow_id = workflow.workflow_id
            group_id = None
        # Return handle for the created run. Ensure that the run base directory
        # is created.
        rundir = self.fs.run_basedir(workflow_id, group_id, run_id)
        util.create_dir(rundir)
        run = RunHandle(
            run_id=run_id,
            workflow_id=workflow_id,
            group_id=group_id,
            arguments=arguments if arguments is not None else list(),
            state_type=st.STATE_PENDING
        )
        self.session.add(run)
        # Update the workflow handle if this is a post-processing run.
        if workflow is not None:
            workflow.postproc_ranking_key = runs
            workflow.postproc_run_id = run_id
        # Commit changes in case run monitors need to access the run state.
        self.session.commit()
        # Set run base directory.
        run.set_rundir(rundir)
        return run

    def delete_run(self, run_id):
        """Delete the entry for the given run from the underlying database.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        flowserv.error.UnknownRunError
        """
        # Get the run handle to ensure that it exists.
        run = self.get_run(run_id)
        # Get base directory for run files
        workflow_id = run.workflow_id
        group_id = run.group_id
        rundir = self.fs.run_basedir(workflow_id, group_id, run_id)
        # Delete run and the base directory containing run files. Commit
        # changes before deleting the directory.
        self.session.delete(run)
        self.session.commit()
        shutil.rmtree(rundir)

    def get_run(self, run_id):
        """Get handle for the given run from the underlying database. Raises an
        error if the run does not exist.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.model.base.RunHandle

        Raises
        ------
        flowserv.error.UnknownRunError
        """
        # Fetch run information from the database. Raises an error if the run
        # is unknown..
        run = self.session\
            .query(RunHandle)\
            .filter(RunHandle.run_id == run_id)\
            .one_or_none()
        if run is None:
            raise err.UnknownRunError(run_id)
        # Set run base directory.
        workflow = run.workflow
        workflow_id = workflow.workflow_id
        workflow.set_staticdir(self.fs.workflow_staticdir(workflow_id))
        rundir = self.fs.run_basedir(run.workflow_id, run.group_id, run_id)
        set_files(run, rundir)
        return run

    def list_runs(self, group_id, state=None):
        """Get list of run handles for all runs that are associated with a
        given workflow group.

        Parameters
        ----------
        group_id: string, optional
            Unique workflow group identifier
        state: string or list(string), default=None
            Run state query. If given, only those runs that are in the given
            state(s) will be returned.

        Returns
        -------
        list(flowserv.model.base.RunHandle)
        """
        # Generate query that returns the handles of all runs. If the state
        # conditions are given, we add further filters.
        query = self.session\
            .query(RunHandle)\
            .filter(RunHandle.group_id == group_id)
        if state is not None:
            if isinstance(state, list):
                query = query.filter(RunHandle.state_type.in_(state))
            else:
                query = query.filter(RunHandle.state_type == state)
        rs = query.all()
        # Set run directory for all elements in the query result.
        runs = list()
        for run in rs:
            rundir = self.fs.run_basedir(
                workflow_id=run.workflow_id,
                group_id=run.group_id,
                run_id=run.run_id
            )
            workflow = run.workflow
            workflow_id = workflow.workflow_id
            workflow.set_staticdir(self.fs.workflow_staticdir(workflow_id))
            set_files(run, rundir)
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
        list(flowserv.model.base.RunHandle)
        """
        if state is not None:
            return self.list_runs(group_id=group_id, state=state)
        else:
            return self.list_runs(group_id=group_id, state=st.ACTIVE_STATES)

    def update_run(self, run_id, state):
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

        Returns
        -------
        flowserv.model.base.RunHandle

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownRunError
        """
        # Fetch run information from the database. Raises an error if the run
        # is unknown..
        run = self.get_run(run_id)
        # Get current state identifier to check whether the state transition is
        # valid.
        current_state = run.state_type
        # Only update the state in the database if the workflow is not pending.
        # For pending workflows an entry is created when the run starts.
        # -- RUNNING ----------------------------------------------------------
        if state.is_running():
            if current_state != st.STATE_PENDING:
                msg = 'cannot start run in {} state'
                raise err.ConstraintViolationError(msg.format(current_state))
            run.started_at = state.started_at
        # -- CANCELED or ERROR ------------------------------------------------
        elif state.is_canceled() or state.is_error():
            if current_state not in st.ACTIVE_STATES:
                msg = 'cannot set run in {} state to error'
                raise err.ConstraintViolationError(msg.format(current_state))
            messages = list()
            for i, msg in enumerate(state.messages):
                messages.append(RunMessage(message=msg, pos=i))
            run.log = messages
            run.started_at = state.started_at
            run.ended_at = state.stopped_at
        # -- SUCCESS ----------------------------------------------------------
        elif state.is_success():
            if current_state not in st.ACTIVE_STATES:
                msg = 'cannot set run in {} state to error state'
                raise err.ConstraintViolationError(msg.format(current_state))
            rundir = run.get_rundir()
            files = list()
            # Create list of output files depending on whether files are
            # specified in the workflow specification or not.
            outspec = run.outputs()
            if outspec is not None:
                # List only existing files for output specifications in the
                # workflow handle.
                for outfile in outspec:
                    filename = os.path.join(rundir, outfile.source)
                    if os.path.exists(filename):
                        f = RunFile(relative_path=outfile.source)
                        f.set_filename(filename)
                        files.append(f)
            else:
                # List all files that were generated by the workflow run as
                # output.
                for f_path in state.files:
                    f = RunFile(relative_path=f_path)
                    f.set_filename(os.path.join(rundir, f_path))
                    files.append(f)
            run.files = files
            run.started_at = state.started_at
            run.ended_at = state.finished_at
            # Parse run result if the associated workflow has a result schema.
            result_schema = run.workflow.result_schema
            if result_schema is not None:
                # Read the results from the result file that is specified in
                # the workflow result schema. If the file is not found we
                # currently do not raise an error.
                f = run.get_file(by_name=result_schema.result_file)
                if f is not None:
                    results = util.read_object(f.filename)
                    # Create a dictionary of result values.
                    values = dict()
                    for col in result_schema.columns:
                        val = util.jquery(doc=results, path=col.jpath())
                        col_id = col.column_id
                        if val is None and col.required:
                            msg = "missing value for '{}'".format(col_id)
                            raise err.ConstraintViolationError(msg)
                        elif val is not None:
                            values[col_id] = col.cast(val)
                    run.result = values
        # -- PENDING ----------------------------------------------------------
        elif current_state != st.STATE_PENDING:
            msg = 'cannot set run in pending state to {}'
            raise err.ConstraintViolationError(msg.format(state.type_id))
        run.state_type = state.type_id
        # Commit changes to database.
        self.session.commit()
        return run


# -- Helper functions ---------------------------------------------------------

def set_files(run, rundir):
    """Set filenames for the run and potential result files.

    Parameters
    ----------
    run: flowserv.model.base.RunHandle
        Handle for a workflow run.
    rundir: string
        Base directory for run files.
    """
    run.set_rundir(rundir)
    if run.is_success():
        for f in run.files:
            f.set_filename(os.path.join(rundir, f.relative_path))
