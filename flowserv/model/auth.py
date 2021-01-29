# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The authentication and authorization module contains methods to authorize
users that have logged in to the system as well as methods to authorize that a
given user can execute a requested action.
"""

from abc import ABCMeta, abstractmethod

from flowserv.model.base import APIKey, GroupObject, RunObject, User

import datetime as dt
import dateutil.parser

import flowserv.error as err


class Auth(metaclass=ABCMeta):
    """Base class for authentication and authorization methods. Different
    authorization policies should override the methods of this class.
    """
    def __init__(self, session):
        """Initialize the database connection.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        """
        self.session = session

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
        flowserv.model.base.User

        Raises
        ------
        flowserv.error.UnauthenticatedAccessError
        """
        # The API key may be None. In this case an error is raised.
        if api_key is None:
            raise err.UnauthenticatedAccessError()
        # Get information for user that that is associated with the API key
        # together with the expiry date of the key. If the API key is unknown
        # or expired raise an error.
        query = self.session.query(User)\
            .filter(User.user_id == APIKey.user_id)\
            .filter(APIKey.value == api_key)
        user = query.one_or_none()
        if user is None:
            raise err.UnauthenticatedAccessError()
        expires = dateutil.parser.parse(user.api_key.expires)
        if expires < dt.datetime.now():
            raise err.UnauthenticatedAccessError()
        return user

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
        flowserv.error.UnknownRunError
        flowserv.error.UnknownWorkflowGroupError
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
        flowserv.error.UnknownRunError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Validate that the combination of arguments is valid.
        if group_id is None and run_id is None:
            raise ValueError('no identifier given')
        elif group_id is not None and run_id is not None:
            raise ValueError('invalid arguments')
        # Depending on which of the parameters is given, check whether the
        # group or the run exists. Raise an error if either does not exist.
        if group_id is not None:
            group = self.session\
                .query(GroupObject)\
                .filter(GroupObject.group_id == group_id)\
                .one_or_none()
            if group is None:
                raise err.UnknownWorkflowGroupError(group_id)
            return group_id
        else:
            run = self.session\
                .query(RunObject)\
                .filter(RunObject.run_id == run_id)\
                .one_or_none()
            if run is None:
                raise err.UnknownRunError(run_id)
            return run.group_id


class DefaultAuthPolicy(Auth):
    """Default implementation for the API's authorization methods."""
    def __init__(self, session):
        """Initialize the database connection.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        """
        super(DefaultAuthPolicy, self).__init__(session)

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
        flowserv.error.UnknownRunError
        flowserv.error.UnknownWorkflowGroupError
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
        group = self.session\
            .query(GroupObject)\
            .filter(GroupObject.group_id == run_group)\
            .one_or_none()
        for member in group.members:
            if member.user_id == user_id:
                return True
        return False


class OpenAccessAuth(Auth):
    """Implementation for the API's authorization policy that gives full access
    to any registered user.
    """
    def __init__(self, session):
        """Initialize the database connection.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        """
        super(OpenAccessAuth, self).__init__(session)

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


# -- Helper Functions ---------------------------------------------------------

def open_access(session):
    """Create an open access policy object."""
    return OpenAccessAuth(session)
