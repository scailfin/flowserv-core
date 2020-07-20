# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

from flowserv.model.base import WorkflowHandle
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.user import UserManager
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager

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
        if engine is None:
            config.SYNC()
            from flowserv.controller.init import init_backend
            engine = init_backend()
        self._engine = engine
        # Set the workflow file system object.
        basedir = config.APP_BASEDIR(basedir)
        self._fs = WorkflowFileSystem(util.create_dir(basedir, abs=True))
        # Set the application identifier.
        self._group_id = config.APP_KEY(key)
        with self._db.session() as session:
            manager = WorkflowGroupManager(session=session, fs=self._fs)
            group = manager.get_group(self._group_id)
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


# -- App commands -------------------------------------------------------------

def install_app(
    name=None, description=None, instructions=None, sourcedir=None,
    repourl=None, specfile=None, db=None, basedir=None
):
    """Create database objects for a application that is defined by a workflow
    template. For each application the workflow is created, a single unser and
    a workflow group that is used to run the application. The unique group
    identifer is returned as the application key.

    Parameters
    ----------
    name: string, optional
        Unique workflow name
    description: string, optional
        Optional short description for display in workflow listings
    instructions: string, optional
        File containing instructions for workflow users.
    sourcedir: string, optional
        Directory containing the workflow static files and the workflow
        template specification.
    repourl: string, optional
        Git repository that contains the the workflow files
    specfile: string, optional
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)
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
            name=name,
            description=description,
            instructions=instructions,
            sourcedir=sourcedir,
            repourl=repourl,
            specfile=specfile,
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