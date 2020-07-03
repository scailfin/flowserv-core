# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Define the classes in the Object-Relational Mapping."""

import json

from sqlalchemy import Boolean, String, Text
from sqlalchemy import Column, ForeignKey, UniqueConstraint, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator, Unicode

import flowserv.model.parameter.base as pb
import flowserv.util as util


"""Base class for all database tables."""
Base = declarative_base()


# -- Custom types -------------------------------------------------------------

class JsonObject(TypeDecorator):
    """Decorator for objects that are stored as serialized JSON strings."""

    impl = Unicode

    def process_literal_param(self, value, dialect):
        """Expects a JSON serializable object."""
        if value is not None:
            return json.dumps(value)

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Create JSON object from string serialization."""
        if value is not None:
            return json.loads(value)


class WorkflowParameters(TypeDecorator):
    """Decorator for workflow parameters that are stored as serialized Json
    objects.
    """

    impl = Unicode

    def process_literal_param(self, value, dialect):
        """Expects a dictionary of parameter declarations."""
        if value is not None:
            return json.dumps([p.to_dict() for p in value.values()])

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Create parameter index from JSON serialization."""
        if value is not None:
            return pb.create_parameter_index(json.loads(value), validate=False)


# -- Association Tables -------------------------------------------------------

group_member = Table(
    'group_member',
    Base.metadata,
    Column('group_id', String(32), ForeignKey('workflow_group.group_id')),
    Column('user_id', String(32), ForeignKey('api_user.user_id'))
)


# -- API User -----------------------------------------------------------------

"""Each API user has a unique internal identifier and a password. If the active
flag is True the user is active, otherwise the user has registered but not been
activated or the user been deleted. In either case an inactive user is not
permitted to login). Each user has a unique name. This name is the identifier
that is visible to the user and that is used for display
"""


class User(Base):
    """Each user that registers with the application has a unique identifier
    and a unique user name associated with them.

    For users that are logged into the system the user handle contains the API
    key that was assigned during login..
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'api_user'

    user_id = Column(
        String(32),
        default=util.get_unique_identifier,
        primary_key=True
    )
    secret = Column(String(512), nullable=False)
    name = Column(String(256), nullable=False)
    active = Column(Boolean, nullable=False, default=False)

    # -- Relationships --------------------------------------------------------
    api_key = relationship(
        'APIKey',
        uselist=False,
        cascade='all, delete, delete-orphan'
    )
    groups = relationship(
        'WorkflowGroup',
        secondary=group_member,
        back_populates='members'
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


# -- Workflow Template --------------------------------------------------------

"""Executable workflow templates. With each template the results of an optional
post-processing step over a set of workflow run results are maintained.
"""


class WorkflowTemplate(Base):
    """Each workflow has a unique name, an optional short descriptor and set of
    instructions. The five main components of the template are (i) the workflow
    specification, (ii) the list of parameter declarations, (iii) a optional
    post-processing workflow, (iv) the optional grouping of parameters into
    modules, and (v) the result schema for workflows that generate metrics for
    individual workflow runs.
    With each workflow a reference to the latest run containing post-processing
    results is maintained. The value is NULL if no post-porcessing workflow is
    defined for the template or if it has not been executed yet.
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'workflow_template'

    workflow_id = Column(
        String(32),
        default=util.get_unique_identifier,
        primary_key=True
    )
    name = Column(String(512), nullable=False, unique=True)
    description = Column(Text)
    instructions = Column(Text)
    workflow_spec = Column(JsonObject, nullable=False)
    parameters = Column(WorkflowParameters)
    modules = Column(Text)
    postproc_spec = Column(JsonObject)
    result_schema = Column(JsonObject)

    # -- Relationships --------------------------------------------------------
    groups = relationship('WorkflowGroup', back_populates='workflow')


# -- Workflow Groups ----------------------------------------------------------

"""Groups bring together users and workflow runs. Groups are primarily intended
for benchmarks. In the case of a benchmark each group can be viewed as an
entry (or submission) to the benchmark.

Each group has a name that uniquely identifies it among all groups for a
workflow template. The group is created by a user (the owner) who can invite
other users as group members.

Each group maintains a list of uploaded files that can be used as inputs to
workflow runs. The different workflow runs in a group represent different
configurations of the workflow. When the group is defined, variations to the
original workflow may be made to the workflow specification and the template
parameter declarations.
"""


class WorkflowGroup(Base):
    """A workflow group associates a set of users with a workflow template. It
    allows to define a group-specific set of parameters for the template.
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'workflow_group'

    group_id = Column(
        String(32),
        default=util.get_unique_identifier,
        primary_key=True
    )
    name = Column(String(512), nullable=False)
    workflow_id = Column(
        String(32),
        ForeignKey('workflow_template.workflow_id')
    )
    owner_id = Column(String(32), ForeignKey('api_user.user_id'))
    parameters = Column(WorkflowParameters, nullable=False)
    workflow_spec = Column(JsonObject, nullable=False)

    UniqueConstraint('workflow_id', 'name')

    # -- Relationships --------------------------------------------------------
    members = relationship(
        'User',
        secondary=group_member,
        back_populates='groups'
    )
    owner = relationship('User', uselist=False)
    uploads = relationship('UploadFile', back_populates='group')
    workflow = relationship('WorkflowTemplate', back_populates='groups')


class UploadFile(Base):
    """Uploaded files are assigned to individual workflow groups. Each file is
    assigned a unique identifier.
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'group_upload_file'

    file_id = Column(
        String(32),
        default=util.get_unique_identifier,
        primary_key=True
    )
    group_id = Column(String(32), ForeignKey('workflow_group.group_id'))
    name = Column(String(512), nullable=False)
    file_type = Column(String(255))

    # -- Relationships --------------------------------------------------------
    group = relationship('WorkflowGroup', back_populates='uploads')
