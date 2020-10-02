# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

import tempfile
import time

from io import BytesIO, StringIO
from typing import Callable, Dict, List, Optional

from flowserv.app.result import RunResult
from flowserv.controller.base import WorkflowController
from flowserv.model.auth import open_access
from flowserv.model.database import DB, SessionScope
from flowserv.model.files.base import (
    DatabaseFile, FileObject, FileStore, IOFile
)
from flowserv.model.files.fs import FSFile
from flowserv.model.parameter.files import is_file, InputFile
from flowserv.model.template.parameter import ParameterIndex
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.service.auth import get_auth
from flowserv.service.api import API
from flowserv.service.files import get_filestore
from flowserv.service.run.argument import ARG, FILE
from flowserv.service.postproc.util import copy_postproc_files

import flowserv.config.app as config
import flowserv.error as err
import flowserv.util as util


class App(object):
    """Application object for single workflow applications. Maintains workflow
    metadata and provides functionality to execute and monitor workflow runs.
    """
    def __init__(
        self, db: Optional[DB] = None,
        engine: Optional[WorkflowController] = None,
        fs: Optional[FileStore] = None, auth: Optional[Callable] = None,
        key: Optional[str] = None
    ):
        """Initialize the associated database and engine to retrieve workflow
        information and execute workflow runs. Each application has a unique
        key which is the identifier of the associated workflow. If the
        application key is not given it is expected in the respective
        environment variable 'FLOWSERV_APP'.

        Parameters
        ----------
        db: flowserv.model.database.DB, default=None
            Database connection manager.
        engine: flowserv.controller.base.WorkflowController, default=None
            Workflow controller to execute application runs.
        auth: callable, default=None
            Fuunction to generate an instance of the authentication policy.
            To generate an Auth instance we need a database session object.
            Thus, we cannot pass a default Auth object (for test purposes) to
            the app class.
        fs: flowserv.model.files.base.FileStore, default=None
            File store to access application files.
        key: string, default=None
            Unique application identifier. If not given the value is expected
            in the environment variable FLOWSERV_APP.
        """
        if db is None:
            # Use the default database object if no database is given.
            from flowserv.service.database import database
            db = database
        self._db = db
        # Set API components.
        if engine is None:
            from flowserv.controller.init import init_backend
            engine = init_backend()
        self._engine = engine
        self._auth = auth if auth is not None else get_auth
        self._fs = fs if fs is not None else get_filestore()
        # Get application properties from the database.
        self._workflow_id = config.APP_KEY(key)
        with self._db.session() as session:
            manager = WorkflowManager(session=session, fs=fs)
            workflow = manager.get_workflow(self._workflow_id)
            self._name = workflow.name
            self._description = workflow.description
            self._instructions = workflow.instructions
            self._parameters = workflow.parameters
            self._postproc_spec = workflow.postproc_spec

    def _api(self, session: SessionScope) -> API:
        """Get an instance of the service API using a given database session.

        Parameters
        ----------
        session: flowserv.model.database.SessionScope
            Database session object.

        Returns
        -------
        flowserv.service.api.API
        """
        return API(
            session=session,
            engine=self._engine,
            auth=self._auth(session),
            fs=self._fs
        )

    def cancel_run(
        self, run_id: str, user_id: Optional[str] = None,
        reason: List[str] = None
    ):
        """Cancel the run with the given identifier.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to cancel the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user_id: string
            Unique user identifier
        reason: string, optional
            Optional text describing the reason for cancelling the run

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        flowserv.error.InvalidRunStateError
        """
        with self._db.session() as session:
            api = self._api(session=session)
            api.runs().cancel_run(run_id=run_id, user_id=user_id)

    def delete_run(self, run_id: str, user_id: Optional[str] = None):
        """Delete the run with the given identifier.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to delete the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user_id: string, default=None
            Unique user identifier

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        flowserv.error.InvalidRunStateError
        """
        with self._db.session() as session:
            api = self._api(session=session)
            api.runs().delete_run(run_id=run_id, user_id=user_id)

    def description(self) -> str:
        """Get descriptive header for the application.

        Returns
        -------
        string
        """
        return self._description

    def get_file(
        self, run_id: str, file_id: str, user_id: Optional[str] = None
    ) -> DatabaseFile:
        """Get buffer, name and mime type for a run result file.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        file_id: string
            Unique file identifier.
        user_id: string, default=None
            Identifier for user that is making the request.

        Returns
        -------
        flowserv.model.files.base.DatabaseFile
        """
        with self._db.session() as session:
            api = self._api(session=session)
            return api.runs().get_result_file(
                run_id=run_id,
                file_id=file_id,
                user_id=user_id
            )

    def get_postproc_results(self):
        """Get results of a post-processing run. The result is None if no
        entry for a post-porcessing run is found in the workflow handle.

        Returns
        -------
        flowserv.app.result.RunResult
        """
        with self._db.session() as session:
            api = self._api(session=session)
            doc = api.workflows().get_workflow(workflow_id=self._workflow_id)
            if 'postproc' in doc:
                return RunResult(doc=doc['postproc'], loader=self.get_file)

    @property
    def identifier(self) -> str:
        """Get the identifier of the associated workflow.

        Returns
        -------
        string
        """
        return self._workflow_id

    def instructions(self) -> str:
        """Get instructions text for the application.

        Returns
        -------
        string
        """
        return self._instructions

    def name(self) -> str:
        """Get application title.

        Returns
        -------
        string
        """
        return self._name

    def parameters(self) -> ParameterIndex:
        """Get parameter declaration for application runs.

        Returns
        -------
        flowserv.model.template.parameters.ParameterIndex
        """
        return self._parameters

    def poll_run(self, run_id, user_id: Optional[str] = None) -> RunResult:
        """Get run result handle for a given run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user_id: string, default=None
            Unique user identifier

        Returns
        -------
        flowserv.app.result.RunResult

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        """
        with self._db.session() as session:
            api = self._api(session=session)
            return RunResult(
                doc=api.runs().get_run(run_id=run_id, user_id=user_id),
                loader=self.get_file
            )

    def prepare_postproc_data(
        self,
        runs: List[RunResult],
        outputdir: Optional[str] = None
    ) -> str:
        """Prepare input data for a post-processing workflow from a given list
        of workflow runs.

        Creates the data in a given output directory. If no directory is given,
        a temporary directory is created. Returns the path to the directory
        containing the collected run result files for the post-processing run.

        Parameters
        ----------
        runs: list(flowserv.app.result.RunResult)
            List of run results that are included in the post processing.
        outputdir: string, default=None
            Output directory where the post-processing data files will be
            created.

        Returns
        -------
        string
        """
        # If no directory was given a temporary directory is created.
        outputdir = outputdir if outputdir is not None else tempfile.mkdtemp()
        # Get the relative path names of the run result files that are expected
        # by the post-processing workflow.
        input_files = self._postproc_spec.get('inputs', {}).get('files', [])
        # Collect required run result files for all runs.
        input_runs = list()
        for run in runs:
            run_id = run.run_id
            run_files = list()
            for key in input_files:
                run_files.append((key, run.get_file(key)))
            input_runs.append((run_id, run_id, run_files))
        # Copy run result files to the output directory.
        copy_postproc_files(
            runs=input_runs,
            outputdir=outputdir
        )
        return outputdir

    def start_run(
        self, arguments: Dict, user_id: Optional[str] = None,
        poll_interval: Optional[int] = None
    ) -> RunResult:
        """Run the associated workflow for the given set of arguments.

        Parameters
        ----------
        arguments: dict
            Dictionary of user-provided arguments.
        user_id: string, default=None
            Identifier for user that is making the request.
        poll_interval: int, default=None
            Optional poll interval that is used to check the state of a run
            until it is no longer in active state.

        Returns
        -------
        flowserv.app.result.RunResult

        Raises
        ------
        flowserv.error.InvalidArgumentError
        flowserv.error.MissingArgumentError
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        flowserv.error.UnknownParameterError
        flowserv.error.UnknownWorkflowGroupError
        """
        with self._db.session() as session:
            api = self._api(session=session)
            # Create a new group for the run.
            group_id = api.groups().create_group(
                workflow_id=self._workflow_id,
                name=util.get_unique_identifier(),
                user_id=user_id
            )['id']
            # Upload any argument values as files that are either of type
            # StringIO or BytesIO.
            arglist = list()
            for key, val in arguments.items():
                # Convert arguments to the format that is expected by the run
                # manager. We pay special attention to file parameters. Input
                # files may be represented as strings, IO buffers or file
                # objects.
                para = self._parameters.get(key)
                if para is None:
                    raise err.UnknownParameterError(key)
                if is_file(para):
                    # Upload a given file prior to running the application.
                    upload_file = None
                    target = None
                    if isinstance(val, str):
                        upload_file = FSFile(val)
                    elif isinstance(val, StringIO):
                        buf = BytesIO(val.read().encode('utf8'))
                        upload_file = IOFile(buf)
                    elif isinstance(val, BytesIO):
                        upload_file = IOFile(val)
                    elif isinstance(val, FileObject):
                        upload_file = val
                    elif isinstance(val, InputFile):
                        upload_file = val.source()
                        target = val.target()
                    else:
                        msg = 'invalid argument {} for {}'.format(key, val)
                        raise err.InvalidArgumentError(msg)
                    fh = api.uploads().upload_file(
                        group_id=group_id,
                        file=upload_file,
                        name=key,
                        user_id=user_id
                    )
                    val = FILE(fh['id'], target=target)
                else:
                    val = para.to_argument(val)
                arglist.append(ARG(key, val))
            # Execute the run and return the serialized run handle.
            run = api.runs().start_run(
                group_id=group_id,
                arguments=arglist,
                user_id=user_id
            )
            rh = RunResult(doc=run, loader=self.get_file)
            # Wait for run to finish if active an poll interval is given.
            while poll_interval and rh.is_active():
                time.sleep(poll_interval)
                rh = self.poll_run(run_id=rh.run_id, user_id=user_id)
            pprun = self.get_postproc_results()
            if pprun is not None:
                while poll_interval and pprun.is_active():
                    time.sleep(poll_interval)
                    pprun = self.get_postproc_results()
            return rh


# -- App commands -------------------------------------------------------------

def install_app(
    source: str, identifier: Optional[str] = None, name: Optional[str] = None,
    description: Optional[str] = None, instructions: Optional[str] = None,
    specfile: Optional[str] = None, manifestfile: Optional[str] = None,
    ignore_postproc: Optional[bool] = False, db: Optional[DB] = None,
    fs: Optional[FileStore] = None
):
    """Create database objects for a application that is defined by a workflow
    template. An application is simply a different representation for the
    workflow that defines the application.

    Parameters
    ----------
    source: string
        Path to local template, name or URL of the template in the repository.
    identifier: string, default=None
        Unique user-provided workflow identifier. If no identifier is given a
        unique identifier will be generated for the application.
    name: string, default=None
        Unique workflow name
    description: string, default=None
        Optional short description for display in workflow listings
    instructions: string, default=None
        File containing instructions for workflow users.
    specfile: string, default=None
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)
    manifestfile: string, default=None
        Path to manifest file. If not given an attempt is made to read one
        of the default manifest file names in the base directory.
    ignore_postproc: bool, default=False
        Ignore post-processing workflow specification if True.
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    fs: flowserv.model.files.base.FileStore, default=None
        File store to access application files.

    Returns
    -------
    string
    """
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    if fs is None:
        fs = get_filestore()
    # Create a new workflow for the application from the specified template.
    with db.session() as session:
        repo = WorkflowManager(session=session, fs=fs)
        workflow = repo.create_workflow(
            source=source,
            identifier=identifier,
            name=name,
            description=description,
            instructions=instructions,
            specfile=specfile,
            manifestfile=manifestfile,
            ignore_postproc=ignore_postproc
        )
        workflow_id = workflow.workflow_id
    return workflow_id


def open_app(
    identifier: Optional[str] = None, db: Optional[DB] = None,
    engine: Optional[WorkflowController] = None,
    fs: Optional[FileStore] = None, auth: Optional[Callable] = None,
) -> App:
    """Get an instance of the flowserv application for a given workflow.

    By default an open access authentication policy is used.

    Parameters
    ----------
    identifier: string, default=None
        Unique application identifier.
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    engine: flowserv.controller.base.WorkflowController, default=None
        Workflow controller to execute application runs.
    auth: callable, default=None
        Fuunction to generate an instance of the authentication policy.
        To generate an Auth instance we need a database session object.
        Thus, we cannot pass a default Auth object (for test purposes) to
        the app class.
    fs: flowserv.model.files.base.FileStore, default=None
        File store to access application files.

    Returns
    -------
    flowserv.app.base.App
    """
    # Use open access authentication by default.
    auth = auth if auth is not None else open_access
    return App(db=db, engine=engine, fs=fs, auth=auth, key=identifier)


def uninstall_app(
    app_key: str, db: Optional[DB] = None, fs: Optional[FileStore] = None
):
    """Remove the workflow that is associated with the given application key.

    Parameters
    ----------
    app_key: string
        Application identifier (i.e., group identifier).
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    fs: flowserv.model.files.base.FileStore, default=None
        File store to access application files.
    """
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    if fs is None:
        fs = get_filestore()
    # Delete workflow and all related files.
    with db.session() as session:
        # Delete workflow using the workflow manager.
        WorkflowManager(session=session, fs=fs).delete_workflow(app_key)
        session.commit()
