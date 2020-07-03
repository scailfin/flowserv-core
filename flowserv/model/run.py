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
    def __init__(self, db, fs):
        """Initialize the connection to the underlying database and the file
        system helper to get path names for run folders.

        Parameters
        ----------
        db: flowserv.model.db.DB
            Database session.
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Helper to generate file system paths to group folders
        """
        self.db = db
        self.fs = fs

    def create_run(self, workflow_id, group_id=None, arguments=None):
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

        Returns
        -------
        flowserv.model.base.RunHandle

        Raises
        ------
        flowserv.error.MissingArgumentError
        """
        # Create a unique run identifier.
        run_id = util.get_unique_identifier()
        # Serialize the given dictionary of workflow arguments (if not None)
        arg_values = dict()
        if arguments is not None:
            for key in arguments:
                arg_values[key] = arguments[key].value
        # Return handle for the created run. Ensure that the run base directory
        # is created.
        util.create_dir(self.fs.run_basedir(workflow_id, group_id, run_id))
        run = RunHandle(
            run_id=run_id,
            workflow_id=workflow_id,
            group_id=group_id,
            arguments=arg_values
        )
        self.db.session.add(run)
        self.db.session.commit()
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
        # Delete run and commit changes.
        self.db.session.delete(run)
        self.db.session.commit()
        # Delete the base directory containing group files
        shutil.rmtree(rundir)

    def get_resource_file(self, run, resource_name):
        """Get path to file represented by a workflow resource.

        Patameters
        ----------
        resource_name: string
            Name of the resource.

        Returns
        -------
        string
        """
        rundir = self.fs.run_basedir(run.workflow_id, run.group_id, run.run_id)
        return os.path.join(rundir, resource_name)

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
        run = self.db.session.query(RunHandle)\
            .filter(RunHandle.run_id == run_id)\
            .one_or_none()
        if run is None:
            raise err.UnknownRunError(run_id)
        return run

    def list_runs(self, group_id):
        """Get list of run handles for all runs that are associated with a
        given workflow group.

        Parameters
        ----------
        group_id: string, optional
            Unique workflow group identifier

        Returns
        -------
        list(flowserv.model.base.RunHandle)
        """
        return self.db.session.query(RunHandle)\
            .filter(RunHandle.group_id == group_id)\
            .all()

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
        # Generate query that returns the handles of all runs in a given group
        # and state. If no state constraint is given the active runs are
        # returned.
        query = self.db.session.query(RunHandle)\
            .filter(RunHandle.group_id == group_id)
        if state is not None:
            query = query.filter(RunHandle.state_type == state)
        else:
            query = query.filter(RunHandle.state_type.in_(st.ACTIVE_STATES))
        return [r.run_id for r in query.all()]

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
            resources = list()
            for resource in state.resources:
                resources.append(RunFile(name=resource.name))
            run.files = resources
            run.started_at = state.started_at
            run.ended_at = state.finished_at
        # -- PENDING ----------------------------------------------------------
        elif current_state != st.STATE_PENDING:
            msg = 'cannot set run in pending state to {}'
            raise err.ConstraintViolationError(msg.format(state.type_id))
        run.state_type = state.type_id
        # Commit changes to database.
        self.db.session.commit()
