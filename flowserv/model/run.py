# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The run manager is used to create, delete, query, and update information
about workflow runs in an underlying database.
"""

from sqlalchemy.orm.session import Session
from typing import List, Optional

import io
import mimetypes
import tarfile

from flowserv.model.base import RunFile, RunObject, RunMessage, WorkflowRankingRun
from flowserv.model.files import FileHandle
from flowserv.model.template.schema import ResultSchema
from flowserv.model.workflow.state import WorkflowState
from flowserv.volume.base import IOBuffer, StorageVolume

import flowserv.error as err
import flowserv.model.files as dirs
import flowserv.model.workflow.state as st
import flowserv.util as util


class RunManager(object):
    """The run manager maintains workflow runs. It provides methods the create,
    delete, and retrieve runs. the manager also provides the functionality to
    update the state of workflow runs.
    """
    def __init__(self, session: Session, fs: StorageVolume):
        """Initialize the connection to the underlying database and the file
        system helper to get path names for run folders.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        fs: flowserv.volume.base.StorageVolume
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
        workflow: flowserv.model.base.WorkflowObject, default=None
            Workflow handle if this is a post-processing run.
        group: flowserv.model.base.GroupObject
            Group handle if this is a group sumbission run.
        arguments: list
            List of argument values for parameters in the template.
        runs: list(string), default=None
            List of run identifier that define the input for a post-processing
            run.

        Returns
        -------
        flowserv.model.base.RunObject

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
        run = RunObject(
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
        rundir = dirs.run_basedir(workflow_id, run_id)
        # Delete run and the base directory containing run files. Commit
        # changes before deleting the directory.
        self.session.delete(run)
        self.session.commit()
        self.fs.delete(key=rundir)

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

    def get_run(self, run_id: str) -> RunObject:
        """Get handle for the given run from the underlying database. Raises an
        error if the run does not exist.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.model.base.RunObject

        Raises
        ------
        flowserv.error.UnknownRunError
        """
        # Fetch run information from the database. Raises an error if the run
        # is unknown..
        run = self.session\
            .query(RunObject)\
            .filter(RunObject.run_id == run_id)\
            .one_or_none()
        if run is None:
            raise err.UnknownRunError(run_id)
        return run

    def get_runarchive(self, run_id: str) -> FileHandle:
        """Get tar archive containing all result files for a given workflow
        run. Raises UnknownRunError if the run is not in SUCCESS state.

        Parameters
        ----------
        run_id: string
            Unique run identifier.

        Returns
        -------
        flowserv.model.files.FileHandle

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
        rundir = dirs.run_basedir(workflow_id=workflow_id, run_id=run_id)
        for f in run.files:
            file = self.fs.load(util.join(rundir, f.key)).open()
            info = tarfile.TarInfo(name=f.key)
            info.size = file.getbuffer().nbytes
            tar_handle.addfile(tarinfo=info, fileobj=file)
        tar_handle.close()
        io_buffer.seek(0)
        # Create file handle for the archive. The file name includes the run
        # identifier. The mime type is 'application/gzip' based on
        # https://superuser.com/questions/901962.
        return FileHandle(
            name='run.{}.tar.gz'.format(run_id),
            mime_type='application/gzip',
            fileobj=IOBuffer(io_buffer)
        )

    def get_runfile(
        self, run_id: str, file_id: str = None, key: str = None
    ) -> FileHandle:
        """Get handle and file object for a given run result file. The file is
        either identified by the unique file identifier or the file key. Raises
        an error if the specified file does not exist.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        file_id: string
            Unique file identifier.

        Returns
        -------
        flowserv.model.files.FileHandle

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
        rundir = dirs.run_basedir(workflow_id=workflow_id, run_id=run_id)
        return FileHandle(
            name=fh.name,
            mime_type=fh.mime_type,
            fileobj=self.fs.load(util.join(rundir, fh.key))
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
        list(flowserv.model.base.RunObject)
        """
        # Generate query that returns the handles of all runs. If the state
        # conditions are given, we add further filters.
        query = self.session\
            .query(RunObject)\
            .filter(RunObject.group_id == group_id)
        if state is not None:
            if isinstance(state, list):
                query = query.filter(RunObject.state_type.in_(state))
            else:
                query = query.filter(RunObject.state_type == state)
        return query.all()

    def list_obsolete_runs(
        self, date: str, state: Optional[str] = None
    ) -> List[RunObject]:
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
        list(flowserv.model.base.RunObject)
        """
        # Get handles for all runs before the given date. Ensure to exclude
        # runs that are part of a current workflow ranking result.
        query = self.session\
            .query(RunObject)\
            .filter(RunObject.created_at < date)\
            .filter(RunObject.run_id.notin_(
                self.session.query(WorkflowRankingRun.run_id)
            ))
        # Add filter for run state if given.
        if state is not None:
            query = query.filter(RunObject.state_type == state)
        return query.all()

    def update_run(
        self, run_id: str, state: WorkflowState,
        runstore: Optional[StorageVolume] = None
    ):
        """Update the state of the given run. This method does check if the
        state transition is valid. Transitions are valid for active workflows,
        if the transition is (a) from pending to running or (b) to an inactive
        state. Invalid state transitions will raise an error.

        For successful runs a reference to the storage volume and the run
        directory containing the result files has to be given.

        Parameters
        ----------
        run_id: string
            Unique identifier for the run
        state: flowserv.model.workflow.state.WorkflowState
            New workflow state
        runstore: flowserv.volume.base.StorageVolume, default=None
            Storage volume containing the run (result) files for a successful
            workflow run.

        Returns
        -------
        flowserv.model.base.RunObject

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
            validate_state_transition(current_state, state.type_id, [st.STATE_PENDING])
            run.started_at = state.started_at
        # -- CANCELED or ERROR ------------------------------------------------
        elif state.is_canceled() or state.is_error():
            validate_state_transition(current_state, state.type_id, st.ACTIVE_STATES)
            messages = list()
            for i, msg in enumerate(state.messages):
                messages.append(RunMessage(message=msg, pos=i))
            run.log = messages
            run.started_at = state.started_at
            run.ended_at = state.stopped_at
        # -- SUCCESS ----------------------------------------------------------
        elif state.is_success():
            validate_state_transition(current_state, state.type_id, st.ACTIVE_STATES)
            assert runstore is not None
            # Archive run files (and remove all other files from the run
            # directory).
            storedir = dirs.run_basedir(
                workflow_id=run.workflow.workflow_id,
                run_id=run_id
            )
            run.files = store_run_files(
                run=run,
                files=state.files,
                source=runstore,
                target=self.fs.get_store_for_folder(key=storedir)
            )
            run.started_at = state.started_at
            run.ended_at = state.finished_at
            # Parse run result if the associated workflow has a result schema.
            # We only expect a result file for runs that are group submissions
            # and not post-processing runs.
            result_schema = run.workflow.result_schema
            if result_schema is not None and run.group_id is not None:
                # Make sure to catch any exceptions in case the run result
                # file is not present or corrupted.
                try:
                    read_run_results(run=run, schema=result_schema, runstore=runstore)
                except Exception:
                    pass
        # -- PENDING ----------------------------------------------------------
        else:
            validate_state_transition(current_state, state.type_id, [st.STATE_PENDING])
        run.state_type = state.type_id
        # Commit changes to database. Then remove the local run directory.
        self.session.commit()
        return run


# -- Helper Functions ---------------------------------------------------------

def store_run_files(
    run: RunObject, files: List[str], source: StorageVolume, target: StorageVolume
) -> List[RunFile]:
    """Create list of output files for a successful run. The list of files
    depends on whether files are specified in the workflow specification or not.
    If files are specified only those files are included in the returned lists.
    Otherwise, all result files that are listed in the run state are returned.

    Parameters
    ----------
    run: flowserv.model.base.RunObject
        Handle for a workflow run.
    files: list of string
        List of result files for a successful workflow run.
    source: flowserv.volume.base.StorageVolume
        Storage volume containing the run (result) files for a successful
        workflow run.
    target: flowserv.volume.base.StorageVolume
        Storage volume for persiting run result files.

    Returns
    -------
    list of RunObject, list of string
    """
    outputs = run.outputs()
    if outputs:
        # List only existing files for output specifications in the
        # workflow handle. Note that (i) the result of run.outputs() is
        # always a dictionary and (ii) that the keys in the returned
        # dictionary are not necessary equal to the file sources.
        files = [f.source for f in run.outputs().values()]
    # Copy files to the target volume.
    runfiles = list()
    for key in files:
        f = source.load(key)
        target.store(file=f, dst=key)
        mime_type, _ = mimetypes.guess_type(url=key)
        runfile = RunFile(
            key=key,
            name=key,
            mime_type=mime_type,
            size=f.size()
        )
        runfiles.append(runfile)
    return runfiles


def read_run_results(run: RunObject, schema: ResultSchema, runstore: StorageVolume):
    """Read the run results from the result file that is specified in the workflow
    result schema. If the file is not found we currently do not raise an error.

    Parameters
    ----------
    run: flowserv.model.base.RunObject
        Handle for a workflow run.
    schema: flowserv.model.template.schema.ResultSchema
        Workflow result schema specification that contains the reference to the
        result file key.
    runstore: flowserv.volume.base.StorageVolume
        Storage volume containing the run (result) files for a successful
        workflow run.
    """
    with runstore.load(schema.result_file).open() as f:
        results = util.read_object(f)
    # Create a dictionary of result values.
    values = dict()
    for col in schema.columns:
        val = util.jquery(doc=results, path=col.jpath())
        col_id = col.column_id
        if val is None and col.required:
            msg = "missing value for '{}'".format(col_id)
            raise err.ConstraintViolationError(msg)
        elif val is not None:
            values[col_id] = col.cast(val)
    run.result = values


def validate_state_transition(current_state: str, target_state: str, valid_states: List[str]):
    """Validate that a transition from current state to target state is
    permitted. The list of valid state identifier determines the current states
    that are permitted to transition to the target state. If an invalid
    transition is detected an error is raised.

    Parameters
    ----------
    current_state: str
        Identifier for the current run state.
    target_state: str
        Identifier for the target workflow state.
    valid_states: list of string
        List of valid source states for the anticipated target state.
    """
    if current_state not in valid_states:
        msg = 'cannot change run in state {} to state {}'
        raise err.ConstraintViolationError(msg.format(current_state, target_state))
