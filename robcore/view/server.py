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

import robcore.view.hateoas as hateoas
import robcore.view.labels as labels


class ServiceSerializer(object):
    """Basic serialization methods that are inherited by the more specific
    serializers for different API resources.
    """
    def __init__(self, urls):
        """Initialize the Url factory.

        Parameters
        ----------
        urls: robcore.view.route.UrlFactory
            Factory for resource Urls
        """
        self.urls = urls

    def service_descriptor(self, name, version, username=None):
        """Serialization of the service descriptor. The descriptor contains the
        service name, version, and a list of HATEOAS references. The optional
        user name indicates whether a request for the service descriptor
        contained a valid access token. If the user name is not None it will be
        included in the service descriptor and the valid token flag is set to
        True.

        Parameters
        ----------
        name: string
            Service name
        version: string
            Service version number
        username: string, optional
            Name of the user that was authenticated by a given access token

        Returns
        -------
        dict
        """
        valid_token = not username is None
        obj = {
            labels.NAME: name,
            labels.VERSION: version,
            labels.VALID_TOKEN: valid_token,
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.service_descriptor(),
                hateoas.LOGIN: self.urls.login(),
                hateoas.LOGOUT: self.urls.logout(),
                hateoas.REGISTER: self.urls.register_user(),
                hateoas.BENCHMARKS: self.urls.list_benchmarks(),
                hateoas.SUBMISSIONS: self.urls.list_submissions()
            })
        }
        if not username is None:
            obj[labels.USERNAME] = username
        return obj
