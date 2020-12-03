# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class to access all API resources. Different types of resourecs are
managed by different components of the API.
"""

from flowserv.service.group import WorkflowGroupService
from flowserv.service.files import UploadFileService
from flowserv.service.run import RunService
from flowserv.service.server import Service
from flowserv.service.user import UserService
from flowserv.service.workflow import WorkflowService


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
        self, service_descriptor: Service, workflow_service: WorkflowService,
        group_service: WorkflowService, upload_service: UploadFileService,
        run_service: RunService, user_service: UserService
    ):
        """Initialize the managers for the different API service components.

        Parameters
        ----------
        service_descriptor: flowserv.service.server.Service
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
        self._service = service_descriptor
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

    def server(self) -> Service:
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

    def users(self):
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
