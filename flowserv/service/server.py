# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""API component that provides information about the service iteself."""

import flowserv.config.api as config
import flowserv.version as version


class Service(object):
    """API component that provides the API sevice descriptor that contains the
    basic information and URLs for the service.
    """
    def __init__(self, serializer, username=None):
        """Initialize the serializer for the service descriptor.  The optional
        user name indicates whether a request for the service descriptor
        contained a valid access token.

        Parameters
        ----------
        serializer: flowserv.view.server.ServiceSerializer
            Service descriptor serializer
        username: string, optional
            Name of the authenticated user

        Raises
        ------
        ValueError
        """
        self.serialize = serializer
        self.username = username

    @property
    def name(self):
        """Each instance of the API should have a (unique) name to identify it.

        Returns
        -------
        string
        """
        return config.API_NAME()

    def service_descriptor(self):
        """Get serialization of descriptor containing the basic information
        about the API.

        Returns
        -------
        dict
        """
        return self.serialize.service_descriptor(
            name=self.name,
            version=self.version,
            username=self.username
        )

    @property
    def version(self):
        """Return the engine API version.

        Returns
        -------
        string
        """
        return version.__version__
