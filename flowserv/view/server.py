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
SERVICE_USER = 'username'
SERVICE_VERSION = 'version'


class ServiceSerializer(object):
    """Default serializer for the service descriptor."""
    def service_descriptor(
        self, name: str, version: str, routes: Dict,
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
        username: string, default=None
            Name of the user that was authenticated by a given access token.

        Returns
        -------
        dict
        """
        obj = {
            SERVICE_NAME: name,
            SERVICE_VERSION: version,
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
