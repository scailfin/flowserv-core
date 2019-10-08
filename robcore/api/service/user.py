# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of API methods that access and manipulate user resources as
well as access tokens.
"""

from robcore.api.serialize.user import UserSerializer
from robcore.api.route import UrlFactory

import robcore.model.user.base as auth


class UserService(object):
    """Implement methods that handle user login and logout as well as
    registration and activation of new users.
    """
    def __init__(self, manager, auth, urls=None, serializer=None):
        """Initialize the user manager that maintains all registered users.

        Parameters
        ----------
        manager: robcore.model.user.base.UserManager
            Manager for registered users
        auth: robcore.model.user.auth.Auth
            Implementation of the authorization policy for the API
        urls: robcore.api.route.UrlFactory
            Factory for API resource Urls
        serializer: robcore.api.serialize.user.UserSerializer, optional
            Override the default serializer
        """
        self.manager = manager
        self.auth = auth
        self.urls = urls if not urls is None else UrlFactory()
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = UserSerializer(self.urls)

    def activate_user(self, user_id):
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
        robcore.model.user.base.error.UnknownUserError
        """
        return self.serialize.user(self.manager.activate_user(user_id))

    def list_users(self, query=None):
        """Get a listing of registered users. The optional query string is used
        to filter users whose name starts with the given string.

        Parameters
        ----------
        query: string, optional
            Prefix string to filter users based on their name.

        Returns
        -------
        dict
        """
        users = self.manager.list_users(query=query)
        return self.serialize.user_listing(users)

    def login_user(self, username, password):
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
        robcore.model.user.base.error.UnknownUserError
        """
        return self.serialize.user(self.manager.login_user(username, password))

    def logout_user(self, user):
        """Logout given user.

        Parameters
        ----------
        user: robcore.model.user.base.UserHandle
            Handle for user that is being logged out

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.UnauthenticatedAccessError
        """
        return self.serialize.user(self.manager.logout_user(user))

    def register_user(self, username, password, verify=False):
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
        verify: bool, optional
            Determines whether the created user is active or inactive

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.ConstraintViolationError
        robcore.error.DuplicateUserError
        """
        user = self.manager.register_user(
            username=username,
            password=password,
            verify=verify
        )
        if verify:
            return self.serialize.registered_user(user)
        else:
            return self.serialize.user(user)

    def request_password_reset(self, username):
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

    def reset_password(self, request_id, password):
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
        robcore.error.ConstraintViolationError
        robcore.error.UnknownRequestError
        """
        user = self.manager.reset_password(
            request_id=request_id,
            password=password
        )
        return self.serialize.user(user)

    def whoami_user(self, access_token):
        """Get serialization of the user that is associated with the given
        access token.

        Parameters
        ----------
        access_token: string
            User access token

        Returns
        -------
        dict
        """
        user = self.auth.authenticate(access_token)
        return self.serialize.user(user)
