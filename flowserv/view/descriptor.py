# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for the service descriptor."""

from typing import Dict, Optional

from flowserv.config import DEFAULT_NAME, FLOWSERV_API_NAME, API_URL

import flowserv.version as version


"""Serialization labels."""
ROUTE_ID = 'id'
ROUTE_PATTERN = 'pattern'

SERVICE_NAME = 'name'
SERVICE_ROUTES = 'routes'
SERVICE_URL = 'url'
SERVICE_USER = 'username'
SERVICE_VERSION = 'version'


class ServiceDescriptorSerializer(object):
    """Default serializer for the service descriptor."""
    def from_config(self, env: Dict, username: Optional[str] = None) -> Dict:
        """Get serialized descriptor with basic information from values in the
        given configuration settings.

        Parameters
        ----------
        env: dict, default=None
            Dictionary that provides access to configuration parameter values.
        username: string, default=None
            Optional name for an authenticated user.

        Returns
        -------
        dict
        """
        doc = {
            SERVICE_NAME: env.get(FLOWSERV_API_NAME, DEFAULT_NAME),
            SERVICE_URL: API_URL(env)
        }
        if username is not None:
            doc[SERVICE_USER] = username
        return doc

    def get_name(self, doc: Dict) -> str:
        """Get the name value from the given document.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.

        Returns
        -------
        string
        """
        return doc.get(SERVICE_NAME)

    def get_routes(self, doc: Dict, routes: Dict) -> str:
        """Get the user name from the given document.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.
        routes: dict
            Dictionary with Url patterns for supported API routes. This will
            override patterns that may be defined in the given serialized
            descriptor.

        Returns
        -------
        string
        """
        doc_routes = dict()
        for obj in doc.get(SERVICE_ROUTES, list()):
            doc_routes[obj[ROUTE_ID]] = obj[ROUTE_PATTERN]
        doc_routes.update(routes)
        return doc_routes

    def get_url(self, doc: Dict) -> str:
        """Get the base Url from the given document.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.

        Returns
        -------
        string
        """
        return doc.get(SERVICE_URL)

    def get_username(self, doc: Dict) -> str:
        """Get the user name from the given document.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.

        Returns
        -------
        string
        """
        return doc.get(SERVICE_USER)

    def get_version(self, doc: Dict) -> str:
        """Get the version information from the given document.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.

        Returns
        -------
        string
        """
        return doc.get(SERVICE_VERSION, version.__version__)

    def service_descriptor(
        self, name: str, version: str, url: str, routes: Dict,
        username: Optional[str] = None
    ) -> Dict:
        """Serialization of the service descriptor. The descriptor contains the
        service name, version, and a list of route patterns. The optional
        user name indicates whether a request for the service descriptor
        contained a valid access token. If the user name is not None it will be
        included in the service descriptor.

        Parameters
        ----------
        name: string
            Service name.
        version: string
            Service version number.
        url: string
            Base Url for the service API. This is the prefix for all Url routes.
        username: string, default=None
            Name of the user that was authenticated by a given access token.

        Returns
        -------
        dict
        """
        obj = {
            SERVICE_NAME: name,
            SERVICE_VERSION: version,
            SERVICE_URL: url,
            SERVICE_ROUTES: [
                {
                    ROUTE_ID: key,
                    ROUTE_PATTERN: value
                } for key, value in routes.items()
            ]
        }
        if username is not None:
            obj[SERVICE_USER] = username
        return obj
