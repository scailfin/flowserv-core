# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class to access all API resources. Different types of resourecs are
managed by different components of the API.
"""

from abc import ABCMeta
from typing import Dict, Optional

from flowserv.config import Config
from flowserv.controller.base import WorkflowController
from flowserv.service.group import WorkflowGroupService
from flowserv.service.files import UploadFileService
from flowserv.service.run import RunService
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.user import UserService
from flowserv.service.workflow import WorkflowService

import flowserv.config as config
import flowserv.view.user as userlabels


class API(object):
    """The API object is a wrapper for the following API components:

        - groups()
        - runs()
        - service()
        - uploads()
        - users()
        - workflows()
    """
    def __init__(
        self, service: ServiceDescriptor, workflow_service: WorkflowService,
        group_service: WorkflowService, upload_service: UploadFileService,
        run_service: RunService, user_service: UserService
    ):
        """Initialize the managers for the different API service components.

        Parameters
        ----------
        service: flowserv.service.descriptor.ServiceDescriptor
            Service descriptor.
        workflow_service: flowserv.service.workflow.WorkflowService
            Manager for workflow resources.
        group_service: flowserv.service.group.WorkflowGroupService
            Managaer for workflow group resources.
        upload_service: flowserv.service.files.UploadFileService
            Managaer for uploaded file resources.
        run_service: flowserv.service.run.RunService
            Manager for workfow run resources.
        user_service: flowserv.service.user.UserService
            Manager for registered user resources.
        """
        self._workflows = workflow_service
        self._service = service
        self._groups = group_service
        self._uploads = upload_service
        self._runs = run_service
        self._users = user_service

    def groups(self) -> WorkflowGroupService:
        """Get API service component that provides functionality to access and
        manipulate workflows groups.

        Returns
        -------
        flowserv.service.group.WorkflowGroupService
        """
        return self._groups

    def runs(self) -> RunService:
        """Get API service component that provides functionality to access
        workflows runs.

        Returns
        -------
        flowserv.service.run.RunService
        """
        return self._runs

    def server(self) -> ServiceDescriptor:
        """Get API component for the service descriptor.

        Returns
        -------
        flowserv.service.server.Service
        """
        return self._service

    def uploads(self) -> UploadFileService:
        """Get API service component that provides functionality to access,
        delete, and upload files for workflows groups.

        Returns
        -------
        flowserv.service.files.UploadFileService
        """
        return self._uploads

    def users(self) -> UserService:
        """Get instance of the user service component.

        Returns
        -------
        flowserv.service.user.UserService
        """
        return self._users

    def workflows(self) -> WorkflowService:
        """Get API service component that provides functionality to access
        workflows and workflow leader boards.

        Returns
        -------
        flowserv.service.workflow.WorkflowService
        """
        return self._workflows


class APIFactory(WorkflowController, Config, metaclass=ABCMeta):
    """Factory pattern for creating API instances. Extends the workflow controller
    with a __call__ method that returns a context manager for creating new
    instances of either a local or remote service API.
    """
    def __init__(self, defaults: Optional[Dict] = None):
        """Initialize the default configuration settings.

        Parameters
        ----------
        defaults: dict, default=None
            Dictionary with default settings.
        """
        if defaults is not None:
            super(APIFactory, self).__init__(defaults)
        else:
            super(APIFactory, self).__init__()

    def login(self, username: str, password: str):
        """Authenticate the user using the given credentials. Updates the
        internal configuration with the returned access token.
        """
        with self() as api:
            doc = api.users().login_user(username=username, password=password)
        self[config.FLOWSERV_ACCESS_TOKEN] = doc[userlabels.USER_TOKEN]

    def logout(self):
        """Delete an access token from the internal configuration."""
        if config.FLOWSERV_ACCESS_TOKEN in self:
            with self() as api:
                api.users().logout_user(self[config.FLOWSERV_ACCESS_TOKEN])
            del self[config.FLOWSERV_ACCESS_TOKEN]
