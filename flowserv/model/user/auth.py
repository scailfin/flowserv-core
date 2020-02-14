# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The authentication and authorization module contains methods to authorize
users that have logged in to the system as well as methods to authorize that a
given user can execute a requested action.
"""

import datetime as dt
import dateutil.parser

from abc import abstractmethod

from flowserv.model.user.base import UserHandle

import flowserv.core.error as err
import flowserv.core.util as util


class Auth(util.ABC):
    """Base class for authentication and authorization methods. Different
    authorization policies should override the methods of this class.
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
        API key. Raises an error if the API key is None or if it is not
        associated with a valid login.

        Parameters
        ----------
        api_key: string
            Unique API access token assigned at login

        Returns
        -------
        flowserv.model.user.base.UserHandle

        Raises
        ------
        flowserv.core.error.UnauthenticatedAccessError
        """
        # The API key may be None. In this case an error is raised.
        if api_key is None:
            raise err.UnauthenticatedAccessError()
        # Get information for user that that is associated with the API key
        # together with the expiry date of the key. If the API key is unknown
        # or expired raise an error.
        sql = (
            'SELECT u.user_id, u.name, k.expires as expires '
            'FROM api_user u, user_key k '
            'WHERE u.user_id = k.user_id AND u.active = 1 AND k.api_key = ?'
        )
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
    def is_group_member(self, user_id, group_id=None, run_id=None):
        """Verify that the given user is member of a workflow group. The group
        is identified either by the given group identifier or by the identifier
        for a run that is associated with the group.

        Expects that exactly one of the two optional identifier is given.
        Raises a ValueError if both identifier are None or both are not None.
        Raises an error if the workflow group or the run is unknown.

        Parameters
        ----------
        user_id: string
            Unique user identifier
        group_id: string, optional
            Unique workflow group identifier
        run_id: string, optional
            Unique run identifier

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        raise NotImplementedError()

    def group_or_run_exists(self, group_id=None, run_id=None):
        """Test whether the given group or run exists. Raises an error if they
        don't exist or if no parameter or both parameters are given.

        Returns the group identifier for the run. If group_id is given the
        value is returned as the result. If the run_id is given the group
        identifier is retrieved as part of the database query.

        Parameters
        ----------
        group_id: string, optional
            Unique workflow group identifier
        run_id: string, optional
            Unique run identifier

        Raises
        ------
        ValueError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Validate that the combination of arguments is valid.
        if group_id is None and run_id is None:
            raise ValueError('no identifier given')
        elif group_id is not None and run_id is not None:
            raise ValueError('invalid arguments')
        # Depending on which of the parameters is given, check whether the
        # group or the run exists. Raise an error if either does not exist.
        if group_id is not None:
            sql = 'SELECT group_id FROM workflow_group WHERE group_id = ?'
            if self.con.execute(sql, (group_id,)).fetchone() is None:
                raise err.UnknownWorkflowGroupError(group_id)
            return group_id
        else:
            sql = 'SELECT run_id, group_id FROM workflow_run WHERE run_id = ?'
            row = self.con.execute(sql, (run_id,)).fetchone()
            if row is None:
                raise err.UnknownRunError(run_id)
            return row['group_id']


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

    def is_group_member(self, user_id, group_id=None, run_id=None):
        """Verify that the given user is member of a workflow group. The group
        is identified either by the given group identifier or by the identifier
        for a run that is associated with the group.

        Expects that exactly one of the two optional identifier is given.
        Raises a ValueError if both identifier are None or both are not None.

        Parameters
        ----------
        user_id: string
            Unique user identifier
        group_id: string, optional
            Unique workflow group identifier
        run_id: string, optional
            Unique run identifier

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Get the group identifier. For post-processing runs the group does
        # not exists. Every user can access results from post-processing runs.
        run_group = super(DefaultAuthPolicy, self).group_or_run_exists(
            group_id=group_id,
            run_id=run_id
        )
        if run_group is None:
            return True
        # Check if the user is a member of the run group.
        sql = (
            'SELECT group_id '
            'FROM group_member '
            'WHERE group_id = ? AND user_id = ?'
        )
        params = (run_group, user_id)
        return self.con.execute(sql, params).fetchone() is not None


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

    def is_group_member(self, user_id, group_id=None, run_id=None):
        """Anyone has access to a workflow group. This method still ensures
        that the combination of argument values is valid and that the group or
        run exists.

        Parameters
        ----------
        user_id: string
            Unique user identifier
        group_id: string, optional
            Unique workflow group identifier
        run_id: string, optional
            Unique run identifier

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        """
        super(OpenAccessAuth, self).group_or_run_exists(
            group_id=group_id,
            run_id=run_id
        )
        return True
