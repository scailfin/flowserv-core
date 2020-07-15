# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for user resources."""

from flowserv.view.base import Serializer


class UserSerializer(Serializer):
    """Default serializer for user resources."""
    def __init__(self, labels=None):
        """Initialize serialization labels.

        Parameters
        ----------
        labels: dict, optional
            Dictionary that contains the values for serialization labels. These
            values will override the respective default labels.
        """
        super(UserSerializer, self).__init__(
            labels={
                'REQUEST_ID': 'requestId',
                'USER_ID': 'id',
                'USER_LIST': 'users',
                'USER_NAME': 'username',
                'USER_TOKEN': 'token'
            },
            override_labels=labels
        )

    def reset_request(self, request_id):
        """Serialization for requested identifier to rest a user password.

        Parameters
        ----------
        request_id: string
            Unique request identifier

        Returns
        -------
        dict
        """
        return {self.labels['REQUEST_ID']: request_id}

    def user(self, user, include_token=True):
        """Serialization for user handle. Contains the user name and the access
        token if the user is logged in.
        Parameters
        ----------
        user: flowserv.model.base.User
            Handle for a registered user
        include_token: bool, optional
            Include API tokens for logged in users if True

        Returns
        -------
        dict
        """
        doc = {
            self.labels['USER_ID']: user.user_id,
            self.labels['USER_NAME']: user.name
        }
        if include_token and user.is_logged_in():
            doc[self.labels['USER_TOKEN']] = user.api_key.value
        return doc

    def user_listing(self, users):
        """Serialize a list of user handles.

        Parameters
        ----------
        users: list(flowserv.model.base.User)
            List of user handles

        Returns
        -------
        dict
        """
        return {self.labels['USER_LIST']: [self.user(u) for u in users]}
