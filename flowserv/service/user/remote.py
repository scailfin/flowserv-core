# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of user service API methods that access and manipulate user
resources as well as access tokens. This implementation provides access to
flowserv objects via a remote RESTful API.
"""

from typing import Dict, Optional

import os

from flowserv.service.user.base import UserService
from flowserv.service.remote import get, post
from flowserv.service.descriptor import ServiceDescriptor

import flowserv.service.descriptor as route
import flowserv.view.user as default_labels

import flowserv.config as config


class RemoteUserService(UserService):
    """HTTP client for a RESTful API to access flowserv API resources."""
    def __init__(self, descriptor: ServiceDescriptor, labels: Optional[Dict] = None):
        """Initialize the Url route patterns from the service descriptor and
        the dictionary of labels for elements in request bodies.

        Parameters
        ----------
        descriptor: flowserv.service.descriptor.ServiceDescriptor
            Service descriptor containing the API route patterns.
        labels: dict, default=None
            Override the default labels for elements in request bodies.
        """
        # Default labels for elements in request bodies.
        self.labels = {
            'REQUEST_ID': default_labels.REQUEST_ID,
            'USER_ID': default_labels.USER_ID,
            'USER_NAME': default_labels.USER_NAME,
            'USER_PASSWORD': 'password',
            'USER_TOKEN': default_labels.USER_TOKEN,
            'VERIFY_USER': 'verify'
        }
        if labels is not None:
            self.labels.update(labels)
        # Short cut to access urls from the descriptor.
        self.urls = descriptor.urls

    def activate_user(self, user_id: str) -> Dict:
        """Activate a new user with the given identifier.

        Parameters
        ----------
        user_id: string
            Unique user name

        Returns
        -------
        dict
        """
        data = {self.labels['USER_ID']: user_id}
        return post(url=self.urls(route.USERS_ACTIVATE), data=data)

    def list_users(self, query: Optional[str] = None) -> Dict:
        """Get a listing of registered users. The optional query string is used
        to filter users whose name starts with the given string.

        Parameters
        ----------
        query: string, default=None
            Prefix string to filter users based on their name.

        Returns
        -------
        dict
        """
        return get(url=self.urls(route.USERS_LIST))

    def login_user(self, username: str, password: str) -> Dict:
        """Get handle for user with given credentials. Raises error if the user
        is unknown or if invalid credentials are provided.

        Parameters
        ----------
        username: string
            Unique name of registered user
        password: string
            User password (in plain text)

        Returns
        -------
        dict
        """
        data = {
            self.labels['USER_NAME']: username,
            self.labels['USER_PASSWORD']: password
        }
        body = post(url=self.urls(route.USERS_LOGIN), data=data)
        # Get the access tokrn from the response body and update the global
        # evironment variable.
        token = body[self.labels['USER_TOKEN']]
        os.environ[config.FLOWSERV_ACCESS_TOKEN] = token
        return body

    def logout_user(self, api_key: str) -> Dict:
        """Logout given user.

        Parameters
        ----------
        api_key: string
            API key for user that is being logged out.

        Returns
        -------
        dict
        """
        return post(url=self.urls(route.USERS_LOGOUT))

    def register_user(
        self, username: str, password: str, verify: Optional[bool] = False
    ) -> Dict:
        """Create a new user for the given username and password. Raises an
        error if a user with that name already exists or if the user name is
        ivalid (e.g., empty or too long).

        Returns success object if user was registered successfully.

        Parameters
        ----------
        username: string
            User email address that is used as the username
        password: string
            Password used to authenticate the user
        verify: bool, default=False
            Determines whether the created user is active or inactive

        Returns
        -------
        dict
        """
        data = {
            self.labels['USER_NAME']: username,
            self.labels['USER_PASSWORD']: password,
            self.labels['VERIFY_USER']: verify
        }
        return post(url=self.urls(route.USERS_REGISTER), data=data)

    def request_password_reset(self, username: str) -> Dict:
        """Request to reset the password for the user with the given name. The
        result contains a unique request identifier for the user to send along
        with their new password.

        Parameters
        ----------
        username: string
            Unique user login name
        """
        data = {self.labels['USER_NAME']: username}
        return post(url=self.urls(route.USERS_PASSWORD_REQUEST), data=data)

    def reset_password(self, request_id: str, password: str) -> Dict:
        """Reset the password for the user that made the given password reset
        request. Raises an error if no such request exists or if the request
        has timed out.

        Returns the serialization of the user handle.

        Parameters
        ----------
        request_id: string
            Unique password reset request identifier
        password: string
            New user password

        Returns
        -------
        dict
        """
        data = {
            self.labels['REQUEST_ID']: request_id,
            self.labels['USER_PASSWORD']: password
        }
        return post(url=self.urls(route.USERS_PASSWORD_RESET), data=data)

    def whoami_user(self, api_key: str) -> Dict:
        """Get serialization of the given user.

        Parameters
        ----------
        api_key: string
            API key for a logged-in user.

        Returns
        -------
        dict
        """
        return get(url=self.urls(route.USERS_WHOAMI))
