# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

from io import BytesIO, StringIO

from flowserv.model.base import WorkflowHandle
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.user import UserManager
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.service.api import API
from flowserv.service.run.argument import ARG, FILE

import flowserv.config.app as config
import flowserv.util as util


class App(object):
    """Application object for single workflow applications. Maintains workflow
    metadata and provides functionality to execute workflow runs. Assumes that
    the given workflow engine is in synchronous mode.
    """
    def __init__(self, db=None, engine=None, basedir=None, key=None):
        """Initialize the associated database and engine to retrieve workflow
        information and execute workflow runs. Each application has a unique
        key which is the identifier of the group that was created when the
        application workflow was installed. If the application key is not
        given it is expected in the respective environment variable.

        Parameters
        ----------
        db: flowserv.model.database.DB, default=None
            Database connection manager.
        engine: flowserv.controller.base.WorkflowController, default=None
            Workflow controller to execute application runs.
        basedir: string, default=None
            Base directory for application files.
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
        self._basedir = config.APP_BASEDIR(basedir)
        if engine is None:
            config.SYNC()
            from flowserv.controller.init import init_backend
            engine = init_backend()
        self._engine = engine
        # Get application properties from the database.
        self._group_id = config.APP_KEY(key)
        with self._db.session() as session:
            manager = WorkflowGroupManager(
                session=session,
                fs=WorkflowFileSystem(util.create_dir(self._basedir, abs=True))
            )
            group = manager.get_group(self._group_id)
            self._user_id = group.owner_id
            self._name = group.name
            workflow = group.workflow
            self._description = workflow.description
            self._instructions = workflow.instructions
            self._parameters = sorted(
                group.parameters.values(),
                key=lambda p: p.index
            )

    def description(self):
        """Get descriptive header for the application.

        Returns
        -------
        string
        """
        return self._description

    def get_file(self, run_id, file_id):
        """Get handle for a run result file with the given identifier.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        file_id: string
            Unique file identifier.

        Returns
        -------
        flowserv.model.base.RunFile
        """
        with self._db.session() as session:
            api = API(
                session=session,
                engine=self._engine,
                basedir=self._basedir
            )
            fh = api.runs().get_result_file(
                run_id=run_id,
                file_id=file_id,
                user_id=self._user_id
            )
            return (fh.filename, fh.mimetype)

    def instructions(self):
        """Get instructions text for the application.

        Returns
        -------
        string
        """
        return self._instructions

    def name(self):
        """Get application title.

        Returns
        -------
        string
        """
        return self._name

    def parameters(self):
        """Get parameter declaration for application runs.

        Returns
        -------
        flowserv.model.template.parameters.ParameterIndex
        """
        return self._parameters

    def run(self, arguments):
        """Run the associated workflow for the given set of arguments.

        Parameters
        ----------
        arguments: dict
            Dictionary of user-provided arguments.

        Returns
        -------
        dict
        """
        with self._db.session() as session:
            api = API(
                session=session,
                engine=self._engine,
                basedir=self._basedir
            )
            # Upload any argument values as files that are either of type
            # StringIO or BytesIO.
            arglist = list()
            for key, val in arguments.items():
                if isinstance(val, StringIO) or isinstance(val, BytesIO):
                    fh = api.uploads().upload_file(
                        group_id=self._group_id,
                        file=val,
                        name=key,
                        user_id=self._user_id
                    )
                    val = FILE(fh['id'])
                arglist.append(ARG(key, val))
            # Execute the run. Since we are using a synchronized engine this
            # will block execution until the run is finished.
            return api.runs().start_run(
                group_id=self._group_id,
                arguments=arglist,
                user_id=self._user_id
            )


# -- App commands -------------------------------------------------------------

def install_app(
    source, name=None, description=None, instructions=None, specfile=None,
    manifestfile=None, db=None, basedir=None
):
    """Create database objects for a application that is defined by a workflow
    template. For each application the workflow is created, a single unser and
    a workflow group that is used to run the application. The unique group
    identifer is returned as the application key.

    Parameters
    ----------
    source: string
        Path to local template, name or URL of the template in the repository.
    name: string, optional
        Unique workflow name
    description: string, optional
        Optional short description for display in workflow listings
    instructions: string, optional
        File containing instructions for workflow users.
    specfile: string, optional
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)
    manifestfile: string, default=None
        Path to manifest file. If not given an attempt is made to read one
        of the default manifest file names in the base directory.
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    basedir: string, default=None
        Base directory for application files.

    Returns
    -------
    string
    """
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    basedir = config.APP_BASEDIR(basedir)
    fs = WorkflowFileSystem(util.create_dir(basedir, abs=True))
    # Create a new workflow for the application from the specified template.
    # For applications, any post-processing workflow is currently ignored.
    with db.session() as session:
        repo = WorkflowManager(session=session, fs=fs)
        workflow = repo.create_workflow(
            source=source,
            name=name,
            description=description,
            instructions=instructions,
            specfile=specfile,
            manifestfile=manifestfile,
            ignore_postproc=True
        )
        workflow_id = workflow.workflow_id
        template = workflow.get_template()
        name = workflow.name
    # Create a default user for the application.
    with db.session() as session:
        manager = UserManager(session=session)
        user = manager.register_user(
            username=util.get_unique_identifier(),
            password=util.get_unique_identifier(),
            verify=False
        )
        user_id = user.user_id
    # Create a single group for the application. This group is used to run
    # the application. The unique group identifier is returned as the
    # application identifier.
    with db.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        group = manager.create_group(
            workflow_id=workflow_id,
            name=name,
            user_id=user_id,
            parameters=template.parameters,
            workflow_spec=template.workflow_spec
        )
        app_id = group.group_id
    return app_id


def list_apps(db=None):
    """List all applications in the database. There currently is no information
    in the database about which workflow is an application or a benchmark. For
    now we use the assumption that applications a workflows with a single group
    that has the same name as the workflow.

    Returns a list of tuples containing the application name and the unique
    application key.

    Parameters
    ----------
    db: flowserv.model.database.DB, default=None
        Database connection manager.

    Returns
    -------
    list
    """
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    result = list()
    with db.session() as session:
        for wf in session.query(WorkflowHandle).all():
            if len(wf.groups) == 1:
                group = wf.groups[0]
                if group.name == wf.name:
                    result.append((wf.name, group.group_id))
    return result


def uninstall_app(app_key, db=None, basedir=None):
    """Remove workflow and group associated with the given application key.

    Parameters
    ----------
    app_key: string
        Application identifier (i.e., group identifier).
    db: flowserv.model.database.DB, default=None
        Database connection manager.
    basedir: string, default=None
        Base directory for application files.
    """
    if db is None:
        # Use the default database object if no database is given.
        from flowserv.service.database import database
        db = database
    basedir = config.APP_BASEDIR(basedir)
    fs = WorkflowFileSystem(util.create_dir(basedir, abs=True))
    # Delete workflow and all related files.
    with db.session() as session:
        # Get the identifier for the workflow that is associated with the
        # application key (workflow group).
        group = WorkflowGroupManager(session=session, fs=fs).get_group(app_key)
        workflow_id = group.workflow_id
        # Delete workflow using the workflow manager.
        WorkflowManager(session=session, fs=fs).delete_workflow(workflow_id)
        session.commit()
