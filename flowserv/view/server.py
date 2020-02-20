# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for the service descriptor."""

from flowserv.view.base import Serializer


class ServiceSerializer(Serializer):
    """Default serializer for the service descriptor."""
    def __init__(self, labels=None):
        """Initialize serialization labels.

        Parameters
        ----------
        labels: object, optional
            Object instance that contains the values for serialization labels
        """
        super(ServiceSerializer, self).__init__(
            labels={
                'SERVICE_NAME': 'name',
                'SERVICE_USER': 'username',
                'SERVICE_TOKEN_VALID': 'validToken',
                'SERVICE_VERSION': 'version',
            },
            override_labels=labels
        )

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
        LABELS = self.labels
        # If the request for the service descriptor contained an API key that
        # was valid, the name for the respective user will be given. The valid
        # token flag indicates that the API key was valid and that the
        # descriptor contains the name of the user.
        valid_token = username is not None
        obj = {
            LABELS['SERVICE_NAME']: name,
            LABELS['SERVICE_VERSION']: version,
            LABELS['SERVICE_TOKEN_VALID']: valid_token
        }
        if username is not None:
            obj[LABELS['SERVICE_USER']] = username
        return obj
