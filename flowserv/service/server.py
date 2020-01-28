# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""API component that provides information about the service iteself."""

from flowserv.view.server import ServiceSerializer

import flowserv.config.api as config
import flowserv.version as version


class Service(object):
    """API component that provides the API sevice descriptor that contains the
    basic information and URLs for the service.
    """
    def __init__(self, urls, serializer=None):
        """Initialize the Url route factory and the serializer for the service
        descriptor.

        Parameters
        ----------
        urls: flowserv.view.route.UrlFactory
            Factory for API resource Urls
        serializer: flowserv.view.server.ServiceSerializer, optional
            Override the default serializer

        Raises
        ------
        ValueError
        """
        self.urls = urls
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = ServiceSerializer(self.urls)

    @property
    def name(self):
        """Each instance of the API should have a (unique) name to identify it.

        Returns
        -------
        string
        """
        return config.API_NAME()

    def service_descriptor(self, username=None):
        """Get serialization of descriptor containing the basic information
        about the API. The optional user name indicates whether a request for
        the service descriptor contained a valid access token.

        Parameters
        ----------
        username: string, optional
            Name of the user that was authenticated by a given access token

        Returns
        -------
        dict
        """
        return self.serialize.service_descriptor(
            name=self.name,
            version=self.version,
            username=username
        )

    @property
    def version(self):
        """Return the engine API version.

        Returns
        -------
        string
        """
        return version.__version__
