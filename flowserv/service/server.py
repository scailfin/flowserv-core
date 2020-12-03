# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""API component that provides information about the service iteself."""

from typing import Dict, Optional

from flowserv.view.server import ServiceSerializer

import flowserv.config.api as config
import flowserv.version as version


"""API routes."""

# Unique identifier for all supported API routes.

FILES_DELETE = 'files:delete'
FILES_DOWNLOAD = 'files:download'
FILES_LIST = 'files:list'
FILES_UPLOAD = 'files:upload'

GROUPS_CREATE = 'groups:create'
GROUPS_DELETE = 'groups:delete'
GROUPS_GET = 'groups:get'
GROUPS_LIST = 'groups:list'
GROUPS_RUNS = 'groups:runs'
GROUPS_UPDATE = 'groups:update'

LEADERBOARD_GET = 'leaderboard'

RUNS_CANCEL = 'runs:cancel'
RUNS_DELETE = 'runs:delete'
RUNS_DOWNLOAD_ARCHIVE = 'runs:download:archive'
RUNS_DOWNLOAD_FILE = 'runs:download:file'
RUNS_GET = 'runs:get'
RUNS_START = 'runs:start'

SERVICE_DESCRIPTOR = 'service'

USERS_ACTIVATE = 'users:activate'
USERS_LIST = 'users:list'
USERS_LOGIN = 'users:login'
USERS_LOGOUT = 'users:logout'
USERS_PASSWORD_REQUEST = 'users:pwd:request'
USERS_PASSWORD_RESET = 'users:pwd:reset'
USERS_REGISTER = 'users:register'
USERS_WHOAMI = 'users:whoami'

WORKFLOWS_GET = 'workflows:get'
WORKFLOWS_GROUPS = 'workflows:groups'
WORKFLOWS_LIST = 'workflows:list'

# Default routes for the API.

ROUTES = {
    FILES_DELETE: 'uploads/{userGroupId}/files/{fileId}',
    FILES_DOWNLOAD: 'uploads/{userGroupId}/files/{fileId}',
    FILES_LIST: 'uploads/{userGroupId}',
    FILES_UPLOAD: 'uploads/{userGroupId}',
    GROUPS_CREATE: 'workflows/{workflowId}/groups',
    GROUPS_DELETE: 'groups/{userGroupId}',
    GROUPS_GET: 'groups/{userGroupId}',
    GROUPS_LIST: 'groups',
    GROUPS_RUNS: 'groups/{userGroupId}/runs',
    GROUPS_UPDATE: 'groups/{userGroupId}',
    LEADERBOARD_GET: 'workflows/{workflowId}/leaderboard',
    RUNS_CANCEL: 'runs/{runId}',
    RUNS_DELETE: 'runs/{runId}',
    RUNS_DOWNLOAD_ARCHIVE: 'runs/{runId}/downloads/archive',
    RUNS_DOWNLOAD_FILE: 'runs/{runId}/downloads/resources/{resourceId}',
    RUNS_GET: 'runs/{runId}',
    RUNS_START: 'groups/{userGroupId}/runs',
    SERVICE_DESCRIPTOR: '',
    USERS_ACTIVATE: 'users/activate',
    USERS_LIST: 'users',
    USERS_LOGIN: 'users/login',
    USERS_LOGOUT: 'users/logout',
    USERS_PASSWORD_REQUEST: 'users/password/request',
    USERS_PASSWORD_RESET: 'users/password/reset',
    USERS_REGISTER: 'users/register',
    USERS_WHOAMI: 'users/whoami',
    WORKFLOWS_GET: 'workflows/{workflowId}',
    WORKFLOWS_GROUPS: 'workflows/{workflowId}/groups',
    WORKFLOWS_LIST: 'workflows'
}


class Service(object):
    """API component that provides the API sevice descriptor that contains the
    basic information and URLs for the service.
    """
    def __init__(
        self, serializer: Optional[ServiceSerializer] = None,
        routes: Optional[Dict] = None, username: Optional[str] = None
    ):
        """Initialize the serializer for the service descriptor.  The optional
        user name indicates whether a request for the service descriptor
        contained a valid access token. Only if the user was authenticated
        successfully a user name will be present.

        Parameters
        ----------
        serializer: flowserv.view.server.ServiceSerializer, default=None
            Service descriptor serializer.
        routes: dict, default=None
            Dictionary with Url patterns for supported API routes.
        username: string, default=None
            Name of the authenticated user.

        Raises
        ------
        ValueError
        """
        self.serialize = serializer if serializer is not None else ServiceSerializer()
        self.routes = routes if routes is not None else ROUTES
        self.username = username

    @property
    def name(self) -> str:
        """Each instance of the API should have a (unique) name to identify it.

        Returns
        -------
        string
        """
        return config.API_NAME()

    def service_descriptor(self) -> Dict:
        """Get serialization of descriptor containing the basic information
        about the API. If the user provided a valid access token then the user
        name will be set and included in the serialized object. If no user
        name is present in the returned dictionary the user is not authenticated
        or authentication is not configured (i.e., open access).

        Returns
        -------
        dict
        """
        return self.serialize.service_descriptor(
            name=self.name,
            version=self.version,
            routes=self.routes,
            username=self.username
        )

    @property
    def version(self) -> str:
        """Return the engine API version.

        Returns
        -------
        string
        """
        return version.__version__
