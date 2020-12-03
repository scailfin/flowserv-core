# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for the service descriptor."""

from typing import Dict, Optional


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
    def get_name(self, doc: Dict, default: str) -> str:
        """Get the name value from the given document. Note that the document
        may be None.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.
        default: string
            Default value if not present in the diven dictionary.

        Returns
        -------
        string
        """
        if doc is not None:
            return doc.get(SERVICE_NAME, default)
        return default

    def get_routes(self, doc: Dict, routes: Dict) -> str:
        """Get the user name from the given document. Note that the document may
        be None. Returns None if the document is None or if the respective element
        is not present.

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
        if doc is not None:
            doc_routes = dict()
            for obj in doc.get(SERVICE_ROUTES, list()):
                doc_routes[obj[ROUTE_ID]] = obj[ROUTE_PATTERN]
            doc_routes.update(routes)
            return doc_routes
        return routes

    def get_url(self, doc: Dict, default: str) -> str:
        """Get the base Url from the given document. Note that the document
        may be None.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.
        default: string
            Default value if not present in the diven dictionary.

        Returns
        -------
        string
        """
        if doc is not None:
            return doc.get(SERVICE_URL, default)
        return default

    def get_username(self, doc: Dict) -> str:
        """Get the user name from the given document. Note that the document may
        be None. Returns None if the document is None or if the respective element
        is not present.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.

        Returns
        -------
        string
        """
        if doc is not None:
            return doc.get(SERVICE_USER)
        return None

    def get_version(self, doc: Dict, default: str) -> str:
        """Get the version information from the given document. Note that the
        document may be None.

        Parameters
        ----------
        doc: dict
            Serialization of a service descriptor.
        default: string
            Default value if not present in the diven dictionary.

        Returns
        -------
        string
        """
        if doc is not None:
            return doc.get(SERVICE_VERSION, default)
        return default

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
