# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Interface for API methods that interact with the user manager."""

from abc import ABCMeta, abstractmethod
from typing import Dict, Optional


class UserService(metaclass=ABCMeta):  # pragma: no cover
    """Specification of methods that handle user login and logout as well as
    registration and activation of new users.
    """
    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()
