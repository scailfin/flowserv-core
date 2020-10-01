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

import io
import mimetypes
import os
import shutil
import tarfile

from typing import IO, List, Optional

from flowserv.model.base import (
    RunFile, RunHandle, RunMessage, WorkflowRankingRun
)
from flowserv.model.files.base import DatabaseFile
from flowserv.model.files.fs import walk
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
        fs: flowserv.model.files.FileStore
            File store for run input and output files.
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
        # Return handle for the created run.
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
            ranking = list()
            for i in range(len(runs)):
                ranking.append(WorkflowRankingRun(run_id=runs[i], rank=i))
            workflow.postproc_ranking = ranking
            workflow.postproc_run_id = run_id
        # Commit changes in case run monitors need to access the run state.
        self.session.commit()
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
        rundir = self.fs.run_basedir(workflow_id, run_id)
        # Delete run and the base directory containing run files. Commit
        # changes before deleting the directory.
        self.session.delete(run)
        self.session.commit()
        self.fs.delete_folder(key=rundir)

    def delete_obsolete_runs(
        self, date: str, state: Optional[str] = None
    ) -> int:
        """Delete all workflow runs that were created before the given date.
        The optional state parameter allows to further restrict the list of
        deleted runs to those that were created before the given date and
        that are in the give state.

        Parameters
        ----------
        date: string
            Filter for run creation date.
        state: string, default=None
            Filter for run state.

        Returns
        -------
        int
        """
        # Get list of obsolete runs and delete each run separately. Count the
        # number of runs to return the total number of deleted runs.
        count = 0
        for run in self.list_obsolete_runs(date=date, state=state):
            self.delete_run(run.run_id)
            count += 1
        return count

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
        return run

    def get_runarchive(self, run_id: str) -> IO:
        """Get tar archive containing all result files for a given workflow
        run. Raises UnknownRunError if the run is not in SUCCESS state.

        Parameters
        ----------
        run_id: string
            Unique run identifier.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownRunError
        """
        # Get the run handle and ensure that the run is in SUCCESS state.
        run = self.get_run(run_id)
        if not run.is_success():
            raise err.UnknownRunError(run_id)
        # Create a memory buffer for the tar file.
        io_buffer = io.BytesIO()
        tar_handle = tarfile.open(fileobj=io_buffer, mode='w:gz')
        # Get file objects for all run result files.
        workflow_id = run.workflow.workflow_id
        rundir = self.fs.run_basedir(workflow_id=workflow_id, run_id=run_id)
        for f in run.files:
            file = self.fs.load_file(os.path.join(rundir, f.key)).open()
            info = tarfile.TarInfo(name=f.key)
            info.size = file.getbuffer().nbytes
            tar_handle.addfile(tarinfo=info, fileobj=file)
        tar_handle.close()
        io_buffer.seek(0)
        return io_buffer

    def get_runfile(
        self, run_id: str, file_id: str = None, key: str = None
    ) -> DatabaseFile:
        """Get handle and file object for a given run result file. The file is
        either identified by the unique file identifier or the file key.Raises
        an error if the specified file does not exist.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        file_id: string
            Unique file identifier.

        Returns
        -------
        flowserv.model.files.base.DatabaseFile

        Raises
        ------
        flowserv.error.UnknownFileError
        ValueError
        """
        # Raise an error if both or neither file_id and key are given.
        if file_id is None and key is None:
            raise ValueError('no arguments for file_id or key')
        elif not (file_id is None or key is None):
            raise ValueError('invalid arguments for file_id and key')
        run = self.get_run(run_id)
        if file_id:
            fh = run.get_file(by_id=file_id)
        else:
            fh = run.get_file(by_key=key)
        if fh is None:
            raise err.UnknownFileError(file_id)
        # Return file handle for resource file
        workflow_id = run.workflow.workflow_id
        rundir = self.fs.run_basedir(workflow_id=workflow_id, run_id=run_id)
        return DatabaseFile(
            name=fh.name,
            mime_type=fh.mime_type,
            fileobj=self.fs.load_file(os.path.join(rundir, fh.key))
        )

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
        return query.all()

    def list_obsolete_runs(
        self, date: str, state: Optional[str] = None
    ) -> List[RunHandle]:
        """List all workflow runs that were created before the given date.
        The optional state parameter allows to further restrict the list of
        returned runs to those that were created before the given date and
        that are in the give state.

        Parameters
        ----------
        date: string
            Filter for run creation date.
        state: string, default=None
            Filter for run state.

        Returns
        -------
        list(flowserv.model.base.RunHandle)
        """
        # Get handles for all runs before the given date. Ensure to exclude
        # runs that are part of a current workflow ranking result.
        query = self.session\
            .query(RunHandle)\
            .filter(RunHandle.created_at < date)\
            .filter(RunHandle.run_id.notin_(
                self.session.query(WorkflowRankingRun.run_id)
            ))
        # Add filter for run state if given.
        if state is not None:
            query = query.filter(RunHandle.state_type == state)
        return query.all()

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

    def update_run(self, run_id, state, rundir=None):
        """Update the state of the given run. This method does check if the
        state transition is valid. Transitions are valid for active workflows,
        if the transition is (a) from pending to running or (b) to an inactive
        state. Invalid state transitions will raise an error.

        For inactive runs a reference to the local file system folder that
        contains run (result) files should be present.

        Parameters
        ----------
        run_id: string
            Unique identifier for the run
        state: flowserv.model.workflow.state.WorkflowState
            New workflow state
        rundir: string, default=None
            Path to folder with run (result) files on the local disk. This
            parameter should be given for all sucessful runs and potentially
            also for runs in an error state.

        Returns
        -------
        flowserv.model.base.RunHandle

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownRunError
        """
        # Fetch run information from the database. Raises an error if the run
        # is unknown. If the run state is the same as the new state we return
        # immediately. The result is None to signal that nothing has changed.
        run = self.get_run(run_id)
        if run.state() == state:
            return None
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
            # Delete all run files.
            if rundir is not None and os.path.exists(rundir):
                try:
                    shutil.rmtree(rundir)
                except PermissionError:
                    pass
        # -- SUCCESS ----------------------------------------------------------
        elif state.is_success():
            if current_state not in st.ACTIVE_STATES:
                msg = 'cannot set run in {} state to error state'
                raise err.ConstraintViolationError(msg.format(current_state))
            assert rundir is not None
            # Create list of output file keyss depending on whether files are
            # specified in the workflow specification or not.
            filekeys = None
            outputs = run.outputs()
            if outputs:
                # List only existing files for output specifications in the
                # workflow handle. Note that (i) the result of run.outputs() is
                # always a dictionary and (ii) that the keys in the returned
                # dictionary are not necessary equal to the file sources.
                filekeys = [f.source for f in run.outputs().values()]
            else:
                # List all files that were generated by the workflow run as
                # output.
                filekeys = state.files
            # For each run file ensure that it exist before adding a file
            # handle to the run. We use the file system store's walk method to
            # get a list of all files that need to be retained for a run.
            walklist = list()
            for filekey in filekeys:
                filename = os.path.join(rundir, filekey)
                if not os.path.exists(filename):
                    continue
                walklist.append((filename, filekey))
            # Get files that will be copied to the file store.
            runfiles = list()
            storefiles = walk(files=walklist)
            for file, filekey in storefiles:
                mime_type, _ = mimetypes.guess_type(url=file.filename)
                rf = RunFile(
                    key=filekey,
                    name=filekey,
                    mime_type=mime_type,
                    size=file.size()
                )
                runfiles.append(rf)
            # Set run properties.
            run.files = runfiles
            run.started_at = state.started_at
            run.ended_at = state.finished_at
            # Parse run result if the associated workflow has a result schema.
            result_schema = run.workflow.result_schema
            if result_schema is not None:
                # Read the results from the result file that is specified in
                # the workflow result schema. If the file is not found we
                # currently do not raise an error.
                filename = os.path.join(rundir, result_schema.result_file)
                if os.path.exists(filename):
                    results = util.read_object(filename)
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
            # Archive run files (and remove all other files from the run
            # directory).
            storedir = self.fs.run_basedir(
                workflow_id=run.workflow.workflow_id,
                run_id=run_id
            )
            self.fs.store_files(files=storefiles, dst=storedir)
            if os.path.exists(rundir):
                # The run directory does not have to exist if the workflow does
                # not access and files or create any files. If the workflow is
                # running in a docker container we may also not have the
                # permissions to delete the directory.
                try:
                    shutil.rmtree(rundir)
                except PermissionError:
                    pass
        # -- PENDING ----------------------------------------------------------
        elif current_state != st.STATE_PENDING:
            msg = 'cannot set run in pending state to {}'
            raise err.ConstraintViolationError(msg.format(state.type_id))
        run.state_type = state.type_id
        # Commit changes to database. Then remove the local run directory.
        self.session.commit()
        return run
