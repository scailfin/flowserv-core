# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of user service API methods that access and manipulate user
resources as well as access tokens. This implementation provides access to
flowserv objects via a remote RESTful API.
"""

from typing import Dict, Optional

import os
import requests

from flowserv.service.user.base import UserService
from flowserv.service.remote import headers
from flowserv.service.route import UrlFactory
from flowserv.view.user import UserSerializer

import flowserv.config.client as config


class RemoteUserService(UserService):
    """HTTP client for a RESTful API to access flowserv API resources."""
    def __init__(self, urls: UrlFactory, serializer: UserSerializer):
        """Initialize the URL factory for RESTful API routes.

        Parameters
        ----------
        urls: flowserv.service.route.UrlFactory
            URL factory for access to API resources.
        serializer: flowserv.view.user.UserSerializer
            Override the default serializer
        """
        self.urls = urls
        self.serializer = serializer

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
        data = {self.serializer.labels['USER_ID']: user_id}
        return self.post(url=self.urls.activate_user(), data=data)

    def get(url: str) -> Dict:
        """Send GET request to given URL and return the JSON body.

        Parameters
        ----------
        url: string
            Request URL.

        Returns
        -------
        dict
        """
        r = requests.get(url, headers=headers())
        r.raise_for_status()
        return r.json()

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
        return self.get(url=self.urls.list_users())

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
            self.serializer.labels['USER_NAME']: username,
            self.serializer.labels['USER_PASSWORD']: password
        }
        body = self.post(url=self.urls.login(), data=data)
        # Get the access tokrn from the response body and update the global
        # evironment variable.
        token = body[self.serializer.labels['USER_TOKEN']]
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
        return self.post(url=self.urls.logout())

    def post(self, url: str, data: Optional[Dict] = None) -> Dict:
        """Send POST request with given (optional) body to a URL. Returns the
        JSON body from the response.

        Parameters
        ----------
        url: string
            Request URL.
        data: dict, default=None
            Optional request body.

        Returns
        -------
        dict
        """
        r = requests.post(url, json=data, headers=headers())
        r.raise_for_status()
        return r.json()

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
            self.serializer.labels['USER_NAME']: username,
            self.serializer.labels['USER_PASSWORD']: password,
            self.serializer.labels['VERIFY_USER']: verify
        }
        return self.post(url=self.urls.register_user(), data=data)

    def request_password_reset(self, username: str) -> Dict:
        """Request to reset the password for the user with the given name. The
        result contains a unique request identifier for the user to send along
        with their new password.

        Parameters
        ----------
        username: string
            Unique user login name
        """
        data = {self.serializer.labels['USER_NAME']: username}
        return self.post(url=self.urls.request_password_reset(), data=data)

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
            self.serializer.labels['REQUEST_ID']: request_id,
            self.serializer.labels['USER_PASSWORD']: password
        }
        return self.post(url=self.urls.reset_password(), data=data)

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
        return self.get(url=self.urls.whoami())
