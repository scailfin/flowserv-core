# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for user resources."""


"""Serialization labels."""
REQUEST_ID = 'requestId'
USER_ID = 'id'
USER_LIST = 'users'
USER_NAME = 'username'
USER_PASSWORD = 'password'
USER_TOKEN = 'token'
VERIFY_USER = 'verify'


class UserSerializer(object):
    """Default serializer for user resources."""
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
        return {REQUEST_ID: request_id}

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
            USER_ID: user.user_id,
            USER_NAME: user.name
        }
        if include_token and user.is_logged_in():
            doc[USER_TOKEN] = user.api_key.value
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
        return {USER_LIST: [self.user(u) for u in users]}
