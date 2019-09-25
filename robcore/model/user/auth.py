# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The authentication and authorization module contains methods to authorize
users that have logged in to the system as well as methods to authorize that a
given user can execute a requested action.
"""

import datetime as dt
import dateutil.parser

from abc import abstractmethod

from robcore.model.user.base import UserHandle

import robcore.error as err


class Auth(object):
    """Base class for authentication and authorization methods. Different
    authorization policies can implement different version of this class.
    """
    def __init__(self, con):
        """Initialize the database connection.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        """
        self.con = con

    def authenticate(self, api_key):
        """Get the unique user identifier that is associated with the given
        API key. Raises an error if the API key is not associated with
        a valid login.

        Parameters
        ----------
        api_key: string
            Unique API access token assigned at login

        Returns
        -------
        robcore.model.user.base.UserHandle

        Raises
        ------
        robcore.error.UnauthenticatedAccessError
        """
        # Get information for user that that is associated with the API key
        # together with the expiry date of the key. If the API key is unknown
        # or expired raise an error.
        sql = 'SELECT u.user_id, u.name, k.expires as expires '
        sql += 'FROM api_user u, user_key k '
        sql += 'WHERE u.user_id = k.user_id AND u.active = 1 AND k.api_key = ?'
        user = self.con.execute(sql, (api_key,)).fetchone()
        if user is None:
            raise err.UnauthenticatedAccessError()
        expires = dateutil.parser.parse(user['expires'])
        if expires < dt.datetime.now():
            raise err.UnauthenticatedAccessError()
        return UserHandle(
            identifier=user['user_id'],
            name=user['name'],
            api_key=api_key
        )

    @abstractmethod
    def is_submission_member(self, user, submission_id=None, run_id=None):
        """Verify that the given user is member of a benchmark submission. The
        submission is is identified either by the givven submission identifier
        or the identifier of a run that is associated with the submission.

        Expects that exactly one of the two optional identifier is given.
        Raises a ValueError if both identifier are None or not None.

        Parameters
        ----------
        user: robcore.model.user.base.UserHandle
            Handle for user that is accessing the resource
        submission_id: string, optional
            Unique submission identifier
        run_id: string, optional
            Unique run identifier

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()


class DefaultAuthPolicy(Auth):
    """Default implementation for the API's authorization methods."""
    def __init__(self, con):
        """Initialize the database connection.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        """
        super(DefaultAuthPolicy, self).__init__(con)

    def is_submission_member(self, user, submission_id=None, run_id=None):
        """Verify that the given user is member of a benchmark submission. The
        submission is is identified either by the givven submission identifier
        or the identifier of a run that is associated with the submission.

        Expects that exactly one of the two optional identifier is given.
        Raises a ValueError if both identifier are None or not None.

        Parameters
        ----------
        user: robcore.model.user.base.UserHandle
            Handle for user that is accessing the resource
        submission_id: string, optional
            Unique submission identifier
        run_id: string, optional
            Unique run identifier

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        """
        if submission_id is None and run_id is None:
            raise ValueError('no identifier given')
        elif not submission_id is None and not run_id is None:
            raise ValueError('two identifier given')
        elif not submission_id is None:
            sql = 'SELECT submission_id FROM submission_member '
            sql += 'WHERE submission_id = ? AND user_id = ?'
            params = (submission_id, user.identifier)
        else:
            sql = 'SELECT r.submission_id '
            sql += 'FROM benchmark_run r, submission_member s '
            sql += 'WHERE r.submission_id = s.submission_id AND '
            sql += 'r.run_id = ? AND user_id = ?'
            params = (run_id, user.identifier)
        return not self.con.execute(sql, params).fetchone() is None


class OpenAccessAuth(Auth):
    """Implementation for the API's authorization policy that gives full access
    to any registered user.
    """
    def __init__(self, con):
        """Initialize the database connection.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        """
        super(OpenAccessAuth, self).__init__(con)

    def is_submission_member(self, user, submission_id=None, run_id=None):
        """Anyone has access to a submission. This method still ensures that
        exactly one of the two optional identifier is given. Raises a
        ValueError if both identifier are None or not None.

        Parameters
        ----------
        user: robcore.model.user.base.UserHandle
            Handle for user that is accessing the resource
        submission_id: string, optional
            Unique submission identifier
        run_id: string, optional
            Unique run identifier

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        """
        if submission_id is None and run_id is None:
            raise ValueError('no identifier given')
        elif not submission_id is None and not run_id is None:
            raise ValueError('two identifier given')
        return True
