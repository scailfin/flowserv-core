# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class to manage information about users that are registered with the
API. Each user has a unique identifier and a display name. The identifier is
generated automatically during user registration. It is used internally to
reference a user.

The user manager contains methods to login and logout users. At login, the user
is assigned an API key that can be used for authentication and authorization in
requests to the API. Each key has a fixed lifespan after which it becomes
invalid. If a user logs out the API key is invalidated immediately.
"""

from passlib.hash import pbkdf2_sha256
from typing import Optional

import datetime as dt
import dateutil.parser

from flowserv.config import DEFAULT_LOGINTTL
from flowserv.model.base import APIKey, PasswordRequest, User

import flowserv.config as config
import flowserv.error as err
import flowserv.util as util


class UserManager(object):
    """The user manager registers new users and handles requests to reset a
    user password. The user manager also allows users to login and to logout.
    A user that is logged in has an API key associated with them. This key is
    valid until a timeout period has passed. When the user logs out the API key
    is invalidated. API keys are stored in an underlying database.
    """
    def __init__(self, session, token_timeout: Optional[int] = DEFAULT_LOGINTTL):
        """Initialize the database connection and the login timeout.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        token_timeout: int, default=24h
            Specifies the period (in seconds) for which an API keys and request
            tokens are valid.
        """
        self.session = session
        self.token_timeout = token_timeout

    def activate_user(self, user_id):
        """Activate the user with the given identifier. A user is active if the
        respective active flag in the underlying database is set to 1.

        Parameters
        ----------
        user_id: string
            Unique user identifier

        Returns
        -------
        flowserv.model.base.User

        Raises
        ------
        flowserv.error.UnknownUserError
        """
        user = self.get_user(user_id)
        if not user.active:
            user.active = True
        return user

    def get_user(self, user_id, active=None):
        """Get handle for specified user. The active parameter allows to put an
        additional constraint on the value of the active property for the user.

        Raises an unknown user error if no matching user exists.

        Returns
        -------
        flowserv.model.base.User

        Raises
        ------
        flowserv.error.UnknownUserError
        """
        query = self.session.query(User).filter(User.user_id == user_id)
        if active is not None:
            query = query.filter(User.active == active)
        user = query.one_or_none()
        if user is None:
            raise err.UnknownUserError(user_id)
        return user

    def list_users(self, prefix=None):
        """Get a listing of registered users. The optional query string is used
        to filter users whose name starts with the given string.

        Parameters
        ----------
        prefix: string, default=None
            Prefix string to filter users based on their name.

        Returns
        -------
        list(flowserv.model.base.User)

        Raises
        ------
        flowserv.error.UnauthenticatedAccessError
        """
        # Construct search query based on whether the query argument is given
        # or not. Ignore the default user in the listing.
        query = self.session.query(User).filter(User.active == True)  # noqa: E712
        query = query.filter(User.user_id != config.DEFAULT_USER)
        if prefix is not None:
            query = query.filter(User.name.like('{}%'.format(prefix)))
        query = query.order_by(User.name)
        # Execute search query and generate result set
        return query.all()

    def login_user(self, username, password):
        """Authorize a given user and assign an API key for them. If the user
        is unknown or the given credentials do not match those in the database
        an unknown user error is raised.

        Returns the API key that has been associated with the user identifier.

        Parameters
        ----------
        username: string
            Unique name (i.e., email address) that the user provided when they
            registered
        password: string
            User password specified during registration (in plain text)

        Returns
        -------
        flowserv.model.base.User

        Raises
        ------
        flowserv.error.UnknownUserError
        """
        # Get the unique user identifier and encrypted password. Raise error
        # if user is unknown
        query = self.session.query(User)\
            .filter(User.name == username)\
            .filter(User.active == True)  # noqa: E712
        user = query.one_or_none()
        if user is None:
            raise err.UnknownUserError(username)
        # Validate that given credentials match the stored user secret
        if not pbkdf2_sha256.verify(password, user.secret):
            raise err.UnknownUserError(username)
        user_id = user.user_id
        ttl = dt.datetime.now() + dt.timedelta(seconds=self.token_timeout)
        # Check if a valid access token is currently associated with the user.
        api_key = user.api_key
        if api_key is not None:
            expires = dateutil.parser.parse(api_key.expires)
            if expires < dt.datetime.now():
                # The key has expired. Set a new key value.
                api_key.value = util.get_unique_identifier()
            api_key.expires = ttl.isoformat()
        else:
            # Create a new API key for the user and set the expiry date. The
            # key expires token_timeout seconds from now.
            user.api_key = APIKey(
                user_id=user_id,
                value=util.get_unique_identifier(),
                expires=ttl.isoformat()
            )
        return user

    def logout_user(self, api_key):
        """Invalidate the API key for the given user. This will logout the user.

        Parameters
        ----------
        user: flowserv.model.base.User
            Handle for user that is being logged out

        Returns
        -------
        flowserv.model.base.User
        """
        # Query the database to get the user handle based on the API key.
        user = self.session.query(User)\
            .join(APIKey)\
            .filter(APIKey.value == api_key)\
            .one_or_none()
        if user is not None:
            # Invalidate the API key by setting it to None.
            user.api_key = None
        return user

    def register_user(self, username, password, verify=False):
        """Create a new user for the given username. Raises an error if a user
        with that name already is registered. Returns the internal unique
        identifier for the created user.

        The verify flag allows to create active or inactive users. An inactive
        user cannot login until they have been activated. This option is
        intended for scenarios where the user receives an email after they
        register that contains a verification/activation link to ensure that
        the provided email address is valid.

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
        flowserv.model.base.User

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.DuplicateUserError
        """
        # Ensure that the password has at least one (non-space) character
        validate_password(password)
        # Ensure that the username is not empty and does not contain more than
        # 512 characters.
        if username is None:
            raise err.ConstraintViolationError('missing user name')
        username = username.strip()
        if username == '' or len(username) > 512:
            raise err.ConstraintViolationError('username too long')
        # If a user with the given username already exists raise an error
        # Get the unique user identifier and encrypted password. Raise error
        # if user is unknown
        query = self.session.query(User).filter(User.name == username)
        user = query.one_or_none()
        if user is not None:
            raise err.DuplicateUserError(username)
        # Insert new user into database after creating an unique user
        # identifier and the password hash.
        user = User(
            user_id=util.get_unique_identifier(),
            name=username,
            secret=pbkdf2_sha256.hash(password.strip()),
            active=False if verify else True
        )
        self.session.add(user)
        return user

    def request_password_reset(self, username):
        """Request a password reset for the user with a given name. Returns
        the request identifier that is required as an argument to reset the
        password. The result is always going to be the identifier string
        independently of whether a user with the given username is registered
        or not.

        Invalidates all previous password reset requests for the user.

        Parameters
        ----------
        username: string
            User email that was provided at registration

        Returns
        -------
        string
        """
        request_id = util.get_unique_identifier()
        # Get user identifier that is associated with the username
        query = self.session.query(User)\
            .filter(User.name == username)\
            .filter(User.active == True)  # noqa: E712
        user = query.one_or_none()
        if user is None:
            return request_id
        # Create new password reset request. The expiry date for the request is
        # calculated using the login timeout
        expires = dt.datetime.now() + dt.timedelta(seconds=self.token_timeout)
        user.password_request = PasswordRequest(
            request_id=request_id,
            expires=expires.isoformat()
        )
        return request_id

    def reset_password(self, request_id, password):
        """Reset the password for the user that made the given password reset
        request. Raises an error if no such request exists or if the request
        has timed out.

        Parameters
        ----------
        request_id: string
            Unique password reset request identifier
        password: string
            New user password

        Returns
        -------
        flowserv.model.base.User

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownRequestError
        """
        # Ensure that the given password is valid
        validate_password(password)
        # Get the user and expiry date for the request. Raise error if the
        # request is unknown or has expired.
        query = self.session.query(User)\
            .filter(User.user_id == PasswordRequest.user_id)\
            .filter(PasswordRequest.request_id == request_id)
        user = query.one_or_none()
        if user is None:
            raise err.UnknownRequestError(request_id)
        expires = dateutil.parser.parse(user.password_request.expires)
        if expires < dt.datetime.now():
            raise err.UnknownRequestError(request_id)
        # Update password hash for the identifier user
        user.secret = pbkdf2_sha256.hash(password.strip())
        # Invalidate all current API keys for the user after password is
        # updated.
        user.api_key = None
        # Remove the request
        user.password_request = None
        # Return handle for user
        return user


# -- Helper Methods -----------------------------------------------------------

def validate_password(password):
    """Validate a given password. Raises constraint violation error if an
    invalid password is given.

    Currently, the only constraint for passwords is that they are not empty

    Parameters
    ----------
    password: string
        User password for authentication

    Raises
    ------
    flowserv.error.ConstraintViolationError
    """
    # Raise error if password is invalid
    if password is None or password.strip() == '':
        raise err.ConstraintViolationError('empty password')
