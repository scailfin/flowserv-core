# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base serializer interface. Includes implementation of base methods that are
used by several different serializers.
"""

import robapi.serialize.hateoas as hateoas
import robapi.serialize.labels as labels


class ServiceSerializer(object):
    """Basic serialization methods that are inherited by the more specific
    serializers for different API resources.
    """
    def __init__(self, urls):
        """Initialize the Url factory.

        Parameters
        ----------
        urls: robapi.service.route.UrlFactory
            Factory for resource Urls
        """
        self.urls = urls

    def service_descriptor(self, name, version):
        """Serialization of the service descriptor. The descriptor contains the
        service name, version, and a list of HATEOAS references.

        Parameters
        ----------
        name: string
            Service name
        version: string
            Service version number

        Returns
        -------
        dict
        """
        return {
            labels.NAME: name,
            labels.VERSION: version,
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.service_descriptor(),
                hateoas.LOGIN: self.urls.login(),
                hateoas.LOGOUT: self.urls.logout(),
                hateoas.REGISTER: self.urls.register_user(),
                hateoas.BENCHMARKS: self.urls.list_benchmarks(),
            })
        }
