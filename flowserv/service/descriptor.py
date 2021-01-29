# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""API component that provides information about the service iteself."""

from __future__ import annotations
from typing import Dict, Optional

from flowserv.view.descriptor import ServiceDescriptorSerializer


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

WORKFLOWS_DOWNLOAD_ARCHIVE = 'workflows:download:archive'
WORKFLOWS_DOWNLOAD_FILE = 'workflows:download:file'
WORKFLOWS_GET = 'workflows:get'
WORKFLOWS_GROUPS = 'workflows:groups'
WORKFLOWS_LIST = 'workflows:list'

# Default routes for the API.

ROUTES = {
    FILES_DELETE: 'uploads/{userGroupId}/files/{fileId}',
    FILES_DOWNLOAD: 'uploads/{userGroupId}/files/{fileId}',
    FILES_LIST: 'uploads/{userGroupId}/files',
    FILES_UPLOAD: 'uploads/{userGroupId}/files',
    GROUPS_CREATE: 'workflows/{workflowId}/groups',
    GROUPS_DELETE: 'groups/{userGroupId}',
    GROUPS_GET: 'groups/{userGroupId}',
    GROUPS_LIST: 'groups',
    GROUPS_RUNS: 'groups/{userGroupId}/runs?state={state}',
    GROUPS_UPDATE: 'groups/{userGroupId}',
    LEADERBOARD_GET: 'workflows/{workflowId}/leaderboard?orderBy={orderBy}&includeAll={includeAll}',
    RUNS_CANCEL: 'runs/{runId}',
    RUNS_DELETE: 'runs/{runId}',
    RUNS_DOWNLOAD_ARCHIVE: 'runs/{runId}/downloads/archive',
    RUNS_DOWNLOAD_FILE: 'runs/{runId}/downloads/files/{fileId}',
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
    WORKFLOWS_DOWNLOAD_ARCHIVE: 'workflows/{workflowId}/downloads/archive',
    WORKFLOWS_DOWNLOAD_FILE: 'workflows/{workflowId}/downloads/files/{fileId}',
    WORKFLOWS_GET: 'workflows/{workflowId}',
    WORKFLOWS_GROUPS: 'workflows/{workflowId}/groups',
    WORKFLOWS_LIST: 'workflows'
}


class ServiceDescriptor(object):
    """API component that provides the API sevice descriptor that contains the
    basic information and supported route patterns for the service.
    """
    def __init__(
        self, doc: Optional[Dict] = None, routes: Optional[Dict] = ROUTES,
        serializer: Optional[ServiceDescriptorSerializer] = None
    ):
        """Initialize the serializer for the service descriptor.

        Parameters
        ----------
        doc: dict, default=None
            Serialized service descriptor. The descriptor may for example be
            retrieved from a remote API.
        routes: dict, default=None
            Dictionary with Url patterns for supported API routes. This will
            override patterns that may be defined in the given serialized
            descriptor.
        serializer: flowserv.view.descriptor.ServiceDescriptorSerializer, default=None
            Service descriptor serializer and deserializer.
        """
        self.serialize = serializer if serializer is not None else ServiceDescriptorSerializer()
        doc = doc
        self.name = self.serialize.get_name(doc)
        self.version = self.serialize.get_version(doc)
        self.url = self.serialize.get_url(doc)
        self._routes = self.serialize.get_routes(doc, routes)
        self.username = self.serialize.get_username(doc)
        # Remove trailing '/' from the url
        while self.url.endswith('/'):
            self.url = self.url[:-1]

    @staticmethod
    def from_config(env: Dict, username: Optional[str] = None) -> ServiceDescriptor:
        """Get descriptor with basic information from values in the given
        configuration settings.

        Parameters
        ----------
        env: dict, default=None
            Dictionary that provides access to configuration parameter values.
        username: string, default=None
            Optional name for an authenticated user.

        Returns
        -------
        flowserv.service.descriptor.ServiceDescriptor
        """
        serializer = ServiceDescriptorSerializer()
        doc = serializer.from_config(env=env, username=username)
        return ServiceDescriptor(doc=doc, serializer=serializer)

    def routes(self) -> Dict:
        """Get dictionary of supported API routes. The returned dictionary maps
        unique route identifiers their route Url pattern.

        Returns
        -------
        dict
        """
        return self._routes

    def to_dict(self) -> Dict:
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
            url=self.url,
            routes=self._routes,
            username=self.username
        )

    def urls(self, key: str, **kwargs) -> str:
        """Get the full Url for the route with the given key.

        Parameters
        ----------
        key: string
            Url route pattern key.
        kwargs: dict
            Optional key word arguments to replace Url pattern variables.

        Returns
        -------
        string
        """
        url_suffix = self.routes().get(key).format(**kwargs)
        return '{}/{}'.format(self.url, url_suffix)
