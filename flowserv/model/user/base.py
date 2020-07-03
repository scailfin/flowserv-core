# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Database objects for registered users."""

from sqlalchemy import Boolean, Column, ForeignKey, String
from sqlalchemy.orm import relationship

from flowserv.model.base import Base

import flowserv.util as util


class User(Base):
    """Each user that registers with the application has a unique identifier
    and a unique user name associated with them.

    For users that are logged into the system the user handle contains the API
    key that was assigned during login..
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'api_user'
    """Each user has a unique identifier, a full name, and an email address.
    The email is expected to be unique as it is currently used as the human-
    readable user identifier.
    """
    user_id = Column(
        String(32),
        default=util.get_unique_identifier,
        primary_key=True
    )
    secret = Column(String(512), nullable=False)
    name = Column(String(256), nullable=False)
    active = Column(Boolean, nullable=False, default=False)
    # api_key = Column(String(32), ForeignKey('api_key.value'))

    # -- Relationships --------------------------------------------------------
    api_key = relationship(
        'APIKey',
        uselist=False,
        cascade='all, delete, delete-orphan'
    )
    password_request = relationship(
        'PasswordRequest',
        uselist=False,
        cascade='all, delete, delete-orphan'
    )

    def is_logged_in(self):
        """Test if the user API key is set as an indicator of whether the user
        is currently logged in or not.

        Returns
        -------
        bool
        """
        return self.api_key is not None


class APIKey(Base):
    """API key assigned to a user at login."""
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'api_key'
    user_id = Column(
        String(32),
        ForeignKey('api_user.user_id'),
        primary_key=True
    )
    value = Column(String(32), default=util.get_unique_identifier, unique=True)
    expires = Column(String(26), nullable=False)


class PasswordRequest(Base):
    """Unique identifier associated with a password reset request."""
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'password_request'
    user_id = Column(
        String(32),
        ForeignKey('api_user.user_id'),
        primary_key=True
    )
    request_id = Column(
        String(32),
        default=util.get_unique_identifier,
        unique=True
    )
    expires = Column(String(26), nullable=False)
