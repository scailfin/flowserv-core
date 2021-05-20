# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper classes method to create instances of the API components. The local
API operates directly on the flowserv database (as opposed to the remote API
that interacts with a local API via a RESTfule API) and provides the ability to
execute workflows on the local machine using an associated workflow engine.

All API components use the same underlying database connection. The connection
object is under the control of of a context manager to ensure that the
connection is closed properly after every API request has been handled.
"""

from typing import Dict, Optional, Tuple

import logging
import os

from flowserv.controller.base import WorkflowController
from flowserv.model.auth import DefaultAuthPolicy, OpenAccessAuth
from flowserv.model.base import RunObject
from flowserv.model.database import DB
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.ranking import RankingManager
from flowserv.model.run import RunManager
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.model.workflow.state import WorkflowState
from flowserv.service.api import API, APIFactory
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.files.local import LocalUploadFileService
from flowserv.service.group.local import LocalWorkflowGroupService
from flowserv.service.run.local import LocalRunService
from flowserv.service.user.local import LocalUserService
from flowserv.service.workflow.local import LocalWorkflowService
from flowserv.volume.base import StorageVolume
from flowserv.volume.factory import Volume

from flowserv.model.user import UserManager

import flowserv.config as config
import flowserv.error as err


"""Define short cuts for environment variable names."""
ACCESS_TOKEN = config.FLOWSERV_ACCESS_TOKEN
AUTH = config.FLOWSERV_AUTH
BASEDIR = config.FLOWSERV_BASEDIR
DATABASE = config.FLOWSERV_DB
WEBAPP = config.FLOWSERV_WEBAPP


class LocalAPIFactory(APIFactory):
    """Factory for context manager that create local API instances. Provides a
    wrapper around the database and the workflow engine.
    """
    def __init__(
        self, env: Optional[Dict] = None, db: Optional[DB] = None,
        engine: Optional[WorkflowController] = None, user_id: Optional[str] = None
    ):
        """Initialize the API factory from a given set of configuration
        parameters and their values. If the configuration dictionary is not
        provided the current values from the respective environment variables
        are used.

        The option to initialize the associated database and workflow engine is
        promarily intended for test purposes.

        Parameters
        ----------
        env: dict, default=None
            Dictionary that provides access to configuration parameter values.
        db: flowserv.model.database.DB, default=None
            Optional default database.
        engine: flowserv.controller.base.WorkflowController, default=None
            Optional workflow controller (for test purposes).
        user_id: string, default=None
            Optional identifier for the authenticated API user.
        """
        # Use the current environment settings if the configuration dictionary
        # is not given.
        env = env if env is not None else config.env()
        super(LocalAPIFactory, self).__init__(env)
        # Ensure that the base directory is set and exists.
        self[BASEDIR] = self.get(BASEDIR, config.API_DEFAULTDIR())
        os.makedirs(self[BASEDIR], exist_ok=True)
        # Initialize that database.
        self._db = db if db is not None else init_db(self)
        # Initialize the workflow engine.
        self._engine = engine if engine is not None else init_backend(self)
        # Initialize the file store.
        self._fs = Volume(doc=self.get(config.FLOWSERV_FILESTORE))
        # Ensure that the authentication policy identifier is set.
        self[AUTH] = self.get(AUTH, config.AUTH_OPEN)
        # Authenticated default user. The initial value depends on the given
        # value for the user_id or authentication policy.
        self._user_id = config.DEFAULT_USER if not user_id and self[AUTH] == config.AUTH_OPEN else user_id

    def __call__(self, user_id: Optional[str] = None, access_token: Optional[str] = None):
        """Get an instance of the context manager that creates the local service
        API instance. Provides the option to initialize the default user for
        the returned API instance or to provide an access token for authentication.

        Parameters
        ----------
        user_id: string, default=None
            Optional identifier for the authenticated API user. This overrides
            the access token and any user_id that was provided when the service
            was instantiated.
        access_token: string, default=None
            Optional access token that is used to authenticate the user. This
            will override the current value for the access token in the local
            configuration.

        Returns
        -------
        flowserv.service.local.SessionManager
        """
        return SessionManager(
            env=self,
            db=self._db,
            engine=self._engine,
            fs=self._fs,
            user_id=user_id if user_id is not None else self._user_id,
            access_token=access_token
        )

    def cancel_run(self, run_id: str):
        """Request to cancel execution of the given run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        flowserv.error.UnknownRunError
        """
        self._engine.cancel_run(run_id=run_id)

    def exec_workflow(
        self, run: RunObject, template: WorkflowTemplate, arguments: Dict,
        staticfs: StorageVolume, config: Optional[Dict] = None
    ) -> Tuple[WorkflowState, StorageVolume]:
        """Initiate the execution of a given workflow template for a set of
        argument values. Returns the state of the workflow and the path to
        the directory that contains run result files for successful runs.

        The client provides a unique identifier for the workflow run that is
        being used to retrieve the workflow state in future calls.

        If the state of the run handle is not pending, an error is raised.

        Parameters
        ----------
        run: flowserv.model.base.RunObject
            Handle for the run that is being executed.
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and
            the parameter declarations.
        arguments: dict
            Dictionary of argument values for parameters in the template.
        staticfs: flowserv.volume.base.StorageVolume
            Storage volume that contains the static files from the workflow
            template.
        config: dict, default=None
            Optional implementation-specific configuration settings that can be
            used to overwrite settings that were initialized at object creation.

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState, flowserv.volume.base.StorageVolume
        """
        return self._engine.exec_workflow(
            run=run,
            template=template,
            arguments=arguments,
            staticfs=staticfs,
            config=config
        )


class SessionManager(object):
    """Context manager that creates a local API and controls the database
    session that is used by all the API components.
    """
    def __init__(
        self, env: Dict, db: DB, engine: WorkflowController, fs: StorageVolume,
        user_id: str, access_token: str
    ):
        """Initialize the object.

        Parameters
        ----------
        env: dict
            Dictionary that provides access to configuration parameter values.
        db: flowserv.model.database.DB
            Database manager.
        engine: flowserv.controller.base.WorkflowController
            Workflow controller used by the API for workflow execution.
        fs: flowserv.volume.base.StorageVolume
            File store for accessing and maintaining files for workflows,
            groups and workflow runs.
        user_id: string
            Identifier of a user that has been authenticated. The value may be
            None.
        access_token: string
            Access token that is used to authenticate the user. The value may
            be None. This will override the value in the respective environment
            variable but not the user identifier if given.
        """
        self._env = env
        self._db = db
        self._engine = engine
        self._fs = fs
        self._user_id = user_id
        self._access_token = access_token
        self._session = None

    def __enter__(self) -> API:
        """Create a new instance of the local API when the context manager is
        entered.
        """
        # Open a new database session.
        self._session = self._db.session()
        session = self._session.open()
        # Shortcuts for local variables.
        env = self._env
        fs = self._fs
        engine = self._engine
        # Start by creating the authorization component and setting the
        # identifier for and authenticated user.
        user_id = self._user_id
        username = None
        if env[AUTH] == config.AUTH_OPEN:
            auth = OpenAccessAuth(session)
            user_id = config.DEFAULT_USER if user_id is None else user_id
        else:
            auth = DefaultAuthPolicy(session)
            access_token = self._access_token if self._access_token is not None else env.get(ACCESS_TOKEN)
            if access_token and user_id is None:
                # If an access token is given we retrieve the user that is
                # associated with the token. Authentication may raise an error.
                # Here, we ignore that error since the token may be an outdated
                # token that is stored in the environment.
                try:
                    user = auth.authenticate(access_token)
                    # Set the user name for the authenticated user (to be
                    # included in the service descriptor).
                    username = user.name
                    user_id = user.user_id
                except err.UnauthenticatedAccessError:
                    pass
        # Create the individual components of the API.
        ttl = env.get(config.FLOWSERV_AUTH_LOGINTTL, config.DEFAULT_LOGINTTL)
        user_manager = UserManager(session=session, token_timeout=ttl)
        run_manager = RunManager(session=session, fs=fs)
        group_manager = WorkflowGroupManager(
            session=session,
            fs=fs,
            users=user_manager
        )
        ranking_manager = RankingManager(session=session)
        workflow_repo = WorkflowManager(session=session, fs=fs)
        return API(
            service=ServiceDescriptor.from_config(env=env, username=username),
            workflow_service=LocalWorkflowService(
                workflow_repo=workflow_repo,
                ranking_manager=ranking_manager,
                group_manager=group_manager,
                run_manager=run_manager,
                user_id=user_id
            ),
            group_service=LocalWorkflowGroupService(
                group_manager=group_manager,
                workflow_repo=workflow_repo,
                backend=engine,
                run_manager=run_manager,
                auth=auth,
                user_id=user_id
            ),
            upload_service=LocalUploadFileService(
                group_manager=group_manager,
                auth=auth,
                user_id=user_id
            ),
            run_service=LocalRunService(
                run_manager=run_manager,
                group_manager=group_manager,
                ranking_manager=ranking_manager,
                backend=engine,
                fs=fs,
                auth=auth,
                user_id=user_id
            ),
            user_service=LocalUserService(
                manager=user_manager,
                auth=auth
            )
        )

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Close the database connection when the context manager exists."""
        self._session.close()
        self._session = None


# -- Helper functions ---------------------------------------------------------

def init_backend(api: APIFactory) -> WorkflowController:
    """Create an instance of the workflow engine based on the given configuration
    settings. The workflow engine receives a reference to the API factory for
    callback operations that modify the global database state.

    Parameters
    ----------
    env: dict, default=None
        Dictionary that provides access to configuration parameter values.
    api: flowserv.service.api.APIFactory
        Reference to tha API factory for callbacks that modify the global
        database state.

    Returns
    -------
    flowserv.controller.base.WorkflowController
    """
    # Create a new instance of the file store based on the configuration in the
    # respective environment variables.
    module_name = api.get(config.FLOWSERV_BACKEND_MODULE)
    class_name = api.get(config.FLOWSERV_BACKEND_CLASS)
    # If both environment variables are None return the default controller.
    # Otherwise, import the specified module and return an instance of the
    # controller class. An error is raised if only one of the two environment
    # variables is set.
    if module_name is None and class_name is None:
        engine = 'flowserv.controller.serial.engine.base.SerialWorkflowEngine'
        logging.info('API backend {}'.format(engine))
        from flowserv.controller.serial.engine.base import SerialWorkflowEngine
        return SerialWorkflowEngine(service=api)
    elif module_name is not None and class_name is not None:
        logging.info('API backend {}.{}'.format(module_name, class_name))
        from importlib import import_module
        module = import_module(module_name)
        return getattr(module, class_name)(service=api)
    raise err.MissingConfigurationError('workflow backend')


def init_db(env: Dict) -> DB:
    """Create an instance of the database object based on the given configuration
    settings. Sets the respective variables to the default value if not set.

    Parameters
    ----------
    env: dict, default=None
        Dictionary that provides access to configuration parameter values.

    Returns
    -------
    flowserv.model.database.DB
    """
    # Get the web app flag. Use True as the default if the value is not set.
    if WEBAPP not in env:
        env[WEBAPP] = True
    web_app = env[WEBAPP]
    # Ensure that the databse connection Url is specified in the configuration.
    url = env.get(DATABASE)
    if url is None:
        # Use a SQLite database in the dabase directory as default.
        # This database needs to be initialized if it does not exist.
        dbfile = '{}/flowserv.db'.format(env[config.FLOWSERV_BASEDIR])
        url = 'sqlite:///{}'.format(dbfile)
        env[DATABASE] = url
        # Maintain a reference to the local database instance for use
        # when creating API instances.
        db = DB(connect_url=url, web_app=web_app)
        if not os.path.isfile(dbfile):
            # Initialize the database if the database if the configuration
            # references the default database and the database file does
            # not exist.
            db.init()
    else:
        # If the database Url is specified in the configuration we create the
        # database object for that Url. In this case we assume that the referenced
        # database has been initialized.
        db = DB(connect_url=env[DATABASE], web_app=web_app)
    # Return the created database object.
    return db
