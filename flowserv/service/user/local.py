# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of user service API methods that access and manipulate user
resources as well as access tokens. This implementation is intended for an API
class that provides access to the flowserv objects directly via a local database
connection.
"""

from typing import Dict, Optional

from flowserv.model.auth import Auth
from flowserv.model.user import UserManager
from flowserv.service.user.base import UserService
from flowserv.view.user import UserSerializer


class LocalUserService(UserService):
    """Implement methods that handle user login and logout as well as
    registration and activation of new users.
    """
    def __init__(
        self, manager: UserManager, auth: Auth,
        serializer: Optional[UserSerializer] = None
    ):
        """Initialize the user manager that maintains all registered users and
        the resource serializer.

        Parameters
        ----------
        manager: flowserv.model.user.UserManager
            Manager for registered users
        auth: flowserv.model.auth.Auth
            Authentication manager.
        serializer: flowserv.view.user.UserSerializer, default=None
            Override the default serializer
        """
        self.manager = manager
        self.auth = auth
        self.serialize = serializer if serializer is not None else UserSerializer()

    def activate_user(self, user_id: str) -> Dict:
        """Activate a new user with the given identifier.

        Parameters
        ----------
        user_id: string
            Unique user name

        Returns
        -------
        dict

        Raises
        ------
        flowserv.model.base.error.UnknownUserError
        """
        return self.serialize.user(self.manager.activate_user(user_id))

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
        users = self.manager.list_users(prefix=query)
        return self.serialize.user_listing(users)

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

        Raises
        ------
        flowserv.model.base.error.UnknownUserError
        """
        return self.serialize.user(self.manager.login_user(username, password))

    def logout_user(self, api_key: str) -> Dict:
        """Logout given user.

        Parameters
        ----------
        api_key: string
            API key for user that is being logged out.

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnauthenticatedAccessError
        """
        return self.serialize.user(self.manager.logout_user(api_key))

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

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.DuplicateUserError
        """
        user = self.manager.register_user(
            username=username,
            password=password,
            verify=verify
        )
        return self.serialize.user(user)

    def request_password_reset(self, username: str) -> Dict:
        """Request to reset the password for the user with the given name. The
        result contains a unique request identifier for the user to send along
        with their new password.

        Parameters
        ----------
        username: string
            Unique user login name

        Returns
        -------
        dict
        """
        request_id = self.manager.request_password_reset(username)
        return self.serialize.reset_request(request_id)

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

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownRequestError
        """
        user = self.manager.reset_password(
            request_id=request_id,
            password=password
        )
        return self.serialize.user(user)

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
        return self.serialize.user(self.auth.authenticate(api_key))
