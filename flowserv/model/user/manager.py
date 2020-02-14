# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
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

import datetime as dt
import dateutil.parser

from passlib.hash import pbkdf2_sha256

from flowserv.model.user.base import UserHandle

import flowserv.config.auth as config
import flowserv.core.error as err
import flowserv.core.util as util


class UserManager(object):
    """The user manager registers new users and handles requests to reset a
    user password. The user manager also allows users to login and to logout.
    A user that is logged in has an API key associated with them. This key is
    valid until a timeout period has passed. When the user logs out the API key
    is invalidated. API keys are stored in an underlying database.
    """
    def __init__(self, con, login_timeout=None):
        """Initialize the database connection and the login timeout.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        login_timeout: int
            Specifies the period (in seconds) for which an API key is valid
            after login
        """
        self.con = con
        if login_timeout is not None:
            self.login_timeout = login_timeout
        else:
            # Get value from the respective environment variable
            self.login_timeout = config.AUTH_LOGINTTL()

    def activate_user(self, user_id):
        """Activate the user with the given identifier. A user is active if the
        respective active flag in the underlying database is set to 1.

        Parameters
        ----------
        user_id: string
            Unique user identifier

        Returns
        -------
        flowserv.model.user.base.UserHandle

        Raises
        ------
        flowserv.core.error.UnknownUserError
        """
        sql = 'SELECT name, active FROM api_user WHERE user_id = ?'
        user = self.con.execute(sql, (user_id,)).fetchone()
        if user is None:
            raise err.UnknownUserError(user_id)
        if user['active'] != 1:
            sql = 'UPDATE api_user SET active = 1 WHERE user_id = ?'
            self.con.execute(sql, (user_id,))
            self.con.commit()
        return UserHandle(identifier=user_id, name=user['name'])

    def list_users(self, query=None):
        """Get a listing of registered users. The optional query string is used
        to filter users whose name starts with the given string.

        Parameters
        ----------
        query: string, optional
            Prefix string to filter users based on their name.

        Returns
        -------
        list(flowserv.model.user.base.UserHandle)

        Raises
        ------
        flowserv.core.error.UnauthenticatedAccessError
        """
        # Construct search query based on whether the query argument is given
        # or not
        sql = 'SELECT user_id, name FROM api_user WHERE active = 1'
        if query is not None:
            sql += ' AND name LIKE ?'
            para = ('{}%'.format(query),)
        else:
            para = ()
        sql += ' ORDER BY name'
        # Execute search query and generate result set
        rs = list()
        for row in self.con.execute(sql, para).fetchall():
            user_id = row['user_id']
            user_name = row['name']
            rs.append(UserHandle(identifier=user_id, name=user_name))
        return rs

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
        flowserv.model.user.base.UserHandle

        Raises
        ------
        flowserv.core.error.UnknownUserError
        """
        # Get the unique user identifier and encrypted password. Raise error
        # if user is unknown
        sql = 'SELECT user_id, name, secret FROM api_user '
        sql += 'WHERE name = ? AND active = 1'
        user = self.con.execute(sql, (username,)).fetchone()
        if user is None:
            raise err.UnknownUserError(username)
        # Validate that given credentials match the stored user secret
        if not pbkdf2_sha256.verify(password, user['secret']):
            raise err.UnknownUserError(username)
        user_id = user['user_id']
        name = user['name']
        ttl = dt.datetime.now() + dt.timedelta(seconds=self.login_timeout)
        # Check if a valid access token is currently associated with the user.
        sql = 'SELECT api_key, expires FROM user_key WHERE user_id = ?'
        rs = self.con.execute(sql, (user_id,)).fetchone()
        if rs is not None:
            api_key = rs['api_key']
            expires = dateutil.parser.parse(rs['expires'])
            if expires >= dt.datetime.now():
                # Update the expiry time for the active access token
                sql = 'UPDATE user_key SET expires=? WHERE user_id = ?'
                self.con.execute(sql, (ttl.isoformat(), user_id))
                return UserHandle(identifier=user_id, name=name, api_key=api_key)
        sql = 'DELETE FROM user_key WHERE user_id = ?'
        self.con.execute(sql, (user_id,))
        # Create a new API key for the user and set the expiry date. The key
        # expires login_timeout seconds from now.
        api_key = util.get_unique_identifier()
        # Insert API key and expiry date into database and return the key
        sql = 'INSERT INTO user_key(user_id, api_key, expires) VALUES(?, ?, ?)'
        self.con.execute(sql, (user_id, api_key, ttl.isoformat()))
        self.con.commit()
        return UserHandle(identifier=user_id, name=name, api_key=api_key)

    def logout_user(self, user):
        """Invalidate the API key for the given user. This will logout the user.

        Parameters
        ----------
        user: flowserv.model.user.base.UserHandle
            Handle for user that is being logged out

        Returns
        -------
        flowserv.model.user.base.UserHandle
        """
        sql = 'DELETE FROM user_key WHERE user_id = ?'
        self.con.execute(sql, (user.identifier,))
        self.con.commit()
        return UserHandle(identifier=user.identifier, name=user.name)

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
        flowserv.model.user.base.UserHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.DuplicateUserError
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
        sql = 'SELECT user_id FROM api_user WHERE name = ?'
        if not self.con.execute(sql, (username,)).fetchone() is None:
            raise err.DuplicateUserError(username)
        # Insert new user into database after creating an unique user identifier
        # and the password hash.
        user_id = util.get_unique_identifier()
        pwd_hash = pbkdf2_sha256.hash(password.strip())
        active = 0 if verify else 1
        sql = 'INSERT INTO api_user(user_id, name, secret, active) '
        sql += 'VALUES(?, ?, ?, ?)'
        self.con.execute(sql, (user_id, username, pwd_hash, active))
        self.con.commit()
        # Log user in after successful registration and return API key
        return UserHandle(identifier=user_id, name=username)

    def request_password_reset(self, username):
        """Request a password reset for the user with a given name. Returns
        the request identifier that is required as an argument to reset the
        password. The result is always going to be the identifier string
        independently of whether a user with the given username is registered or
        not.

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
        sql = 'SELECT user_id FROM api_user WHERE name = ? AND active = 1'
        user = self.con.execute(sql, (username,)).fetchone()
        if user is None:
            return request_id
        user_id = user['user_id']
        # Delete any existing password reset request for the given user
        sql = 'DELETE FROM password_request WHERE user_id = ?'
        self.con.execute(sql, (user_id,))
        # Insert new password reset request. The expiry date for the request is
        # calculated using the login timeout
        expires = dt.datetime.now() + dt.timedelta(seconds=self.login_timeout)
        sql = 'INSERT INTO password_request(user_id, request_id, expires) '
        sql += 'VALUES(?, ?, ?)'
        self.con.execute(sql, (user_id, request_id, expires.isoformat()))
        self.con.commit()
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
        flowserv.model.user.base.UserHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownRequestError
        """
        # Ensure that the given password is valid
        validate_password(password)
        # Get the user and expiry date for the request. Raise error if the
        # request is unknown or has expired.
        sql = 'SELECT u.user_id, u.name, r.expires '
        sql += 'FROM api_user u, password_request r '
        sql += 'WHERE u.user_id = r.user_id AND r.request_id = ?'
        req = self.con.execute(sql, (request_id,)).fetchone()
        if req is None:
            raise err.UnknownRequestError(request_id)
        expires = dateutil.parser.parse(req['expires'])
        if expires < dt.datetime.now():
            raise err.UnknownRequestError(request_id)
        # Update password hash for the identifier user
        user_id = req['user_id']
        username = req['name']
        pwd_hash = pbkdf2_sha256.hash(password.strip())
        sql = 'UPDATE api_user SET secret = ? WHERE user_id = ?'
        self.con.execute(sql, (pwd_hash, user_id))
        # Invalidate all current API keys for the user after password is updated
        sql = 'DELETE FROM user_key WHERE user_id = ?'
        self.con.execute(sql, (user_id,))
        # Remove the request
        sql = 'DELETE FROM password_request WHERE request_id = ?'
        self.con.execute(sql, (request_id,))
        self.con.commit()
        # Return handle for user
        return UserHandle(identifier=user_id, name=username)


# -- Helper Methods ------------------------------------------------------------

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
    flowserv.core.error.ConstraintViolationError
    """
    # Raise error if password is invalid
    if password is None or password.strip() == '':
        raise err.ConstraintViolationError('empty password')
