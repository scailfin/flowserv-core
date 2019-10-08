# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""API component that provides information about the service iteself."""

from robcore.api.serialize.server import ServiceSerializer
from robcore.api.route import UrlFactory

import robcore.config.api as config
import robcore.version as version


class Service(object):
    """API component that provides the API sevice descriptor that contains the
    basic information and URLs for the service.
    """
    def __init__(self, urls=None, serializer=None):
        """Initialize the Url route factory and the serializer for the service
        descriptor.

        Parameters
        ----------
        urls: robcore.api.route.UrlFactory
            Factory for API resource Urls
        serializer: robcore.api.serialize.server.ServiceSerializer, optional
            Override the default serializer

        Raises
        ------
        ValueError
        """
        self.urls = urls if not urls is None else UrlFactory()
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

    def service_descriptor(self):
        """Get serialization of descriptor containing the basic information
        about the API.

        Returns
        -------
        dict
        """
        return self.serialize.service_descriptor(
            name=self.name,
            version=self.version
        )

    @property
    def version(self):
        """Return the engine API version.

        Returns
        -------
        string
        """
        return version.__version__
