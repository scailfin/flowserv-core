# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Define the classes in the Object-Relational Mapping."""

import json
import mimetypes
import os
import shutil

from datetime import datetime
from sqlalchemy import Boolean, Integer, String, Text
from sqlalchemy import Column, ForeignKey, UniqueConstraint, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator, Unicode

from flowserv.model.parameter.base import ParameterGroup
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.template.files import WorkflowOutputFile
from flowserv.model.template.parameter import ParameterIndex
from flowserv.model.template.schema import ResultSchema

import flowserv.model.workflow.state as st
import flowserv.util as util

# -- ORM Base -----------------------------------------------------------------

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
            return json.dumps(value.to_dict())

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Create parameter index from JSON serialization."""
        if value is not None:
            return ParameterIndex.from_dict(json.loads(value), validate=False)


class WorkflowModules(TypeDecorator):
    """Decorator for workflow parameters groups that are stored as serialized
    Json objects.
    """

    impl = Unicode

    def process_literal_param(self, value, dialect):
        """Expects a list of workflow module objects."""
        if value is not None:
            return json.dumps([m.to_dict() for m in value])

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Create workflow module list from JSON serialization."""
        if value is not None:
            return [ParameterGroup.from_dict(m) for m in json.loads(value)]


class WorkflowResultSchema(TypeDecorator):
    """Decorator for the workflow result schema that is stored as serialized
    Json object.
    """

    impl = Unicode

    def process_literal_param(self, value, dialect):
        """Expects a workflow result schema object."""
        if value is not None:
            return json.dumps(value.to_dict())

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Create result schema from JSON serialization."""
        if value is not None:
            return ResultSchema.from_dict(json.loads(value))


class WorkflowOutputs(TypeDecorator):
    """Decorator for workflow output file specifications that are stored as
    serialized Json objects.
    """

    impl = Unicode

    def process_literal_param(self, value, dialect):
        """Expects a list of workflow output file objects."""
        if value is not None:
            return json.dumps([f.to_dict() for f in value])

    process_bind_param = process_literal_param

    def process_result_value(self, value, dialect):
        """Create workflow output file list from JSON serialization."""
        if value is not None:
            return [WorkflowOutputFile.from_dict(f) for f in json.loads(value)]


# -- Files --------------------------------------------------------------------

class FileHandle(Base):
    """The file handle base class provides additional methods to access a file
    and its properties."""

    __abstract__ = True
    _path = None

    @property
    def created_at(self):
        """Datetime timestamp for file creation.

        Returns
        -------
        string
        """
        assert self._path is not None
        ts = os.path.getmtime(self._path)
        ts = util.from_utc_datetime(datetime.utcfromtimestamp(ts))
        return ts.isoformat()

    def created_at_local_time(self):
        """Get string representation of the creation timestamp in the local
        timezone.

        Returns
        -------
        string
        """
        assert self._path is not None
        ts = os.path.getmtime(self._path)
        ts = util.from_utc_datetime(datetime.utcfromtimestamp(ts))
        return ts.isoformat()[:-7]

    def delete(self):
        """Remove the associated file or directory on disk (if it exists)."""
        assert self._path is not None
        if os.path.isfile(self._path):
            os.remove(self._path)
        elif os.path.isdir(self._path):
            shutil.rmtree(self._path)

    @property
    def filename(self):
        """Get path to file on disk."""
        assert self._path is not None
        return self._path

    @property
    def mimetype(self):
        """Get mimetype for file. The type is guessed from the fname."""
        mimetype, _ = mimetypes.guess_type(url=self.name)
        return mimetype

    def set_filename(self, path):
        """Set path to file on disk."""
        self._path = path
        return self

    @property
    def size(self):
        """Get size of file in bytes.

        Returns
        -------
        int
        """
        assert self._path is not None
        return os.stat(self._path).st_size


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

    user_id = Column(String(32), primary_key=True)
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
        'GroupHandle',
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
    expires = Column(String(32), nullable=False)


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
    expires = Column(String(32), nullable=False)


# -- Workflow Template --------------------------------------------------------

"""Executable workflow templates. With each template the results of an optional
post-processing step over a set of workflow run results are maintained.
"""


class WorkflowHandle(Base):
    """Each workflow has a unique name, an optional short descriptor and long
    instruction text. The five main components of the template are (i) the
    workflow specification, (ii) the list of parameter declarations, (iii) an
    optional post-processing workflow specification, (iv) the optional grouping
    of parameters into modules, and (v) the result schema for workflows that
    generate metrics for individual workflow runs.

    With each workflow a reference to the latest run containing post-processing
    results is maintained. The value is NULL if no post-porcessing workflow is
    defined for the template or if it has not been executed yet. The post-
    processing key contains the sorted list of identifier for the runs that
    were used as input to generate the post-processing results.
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
    modules = Column(WorkflowModules)
    outputs = Column(WorkflowOutputs)
    postproc_ranking_key = Column(JsonObject)
    # Omit foreign key here to avaoid circular dependencies. The run will
    # reference the workflow to ensure integrity with respect to deleting
    # the workflow and all dependend runs.
    postproc_run_id = Column(String(32), nullable=True)
    postproc_spec = Column(JsonObject)
    result_schema = Column(WorkflowResultSchema)

    # -- Internal variables ---------------------------------------------------
    _staticdir = None

    # -- Relationships --------------------------------------------------------
    groups = relationship(
        'GroupHandle',
        back_populates='workflow',
        cascade='all, delete, delete-orphan'
    )
    runs = relationship(
        'RunHandle',
        back_populates='workflow',
        cascade='all, delete, delete-orphan'
    )

    def get_template(self, workflow_spec=None, parameters=None):
        """Get template for the workflow. The optional parameters allow to
        override the default values with group-specific values.

        Parameters
        ----------
        workflow_spec: dict, default=None
            Modified workflow specification.
        parameters: dict(flowserv.model.parameter.base.ParameterBase)
            Modified wokflow parameter list.

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate
        """
        assert self._staticdir is not None
        return WorkflowTemplate(
            workflow_spec=self.workflow_spec if workflow_spec is None else workflow_spec,  # noqa: E501
            sourcedir=self._staticdir,
            parameters=self.parameters if parameters is None else parameters,
            modules=self.modules,
            outputs=self.outputs,
            postproc_spec=self.postproc_spec,
            result_schema=self.result_schema
        )

    def set_staticdir(self, dirname):
        """Set the path to the directory containing static template files.

        Parameters
        ----------
        dirname: string
            Path to directory for staic workflow files.
        """
        self._staticdir = dirname


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


class GroupHandle(Base):
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
    runs = relationship(
        'RunHandle',
        back_populates='group',
        cascade='all, delete, delete-orphan'
    )
    uploads = relationship(
        'UploadFile',
        back_populates='group',
        cascade='all, delete, delete-orphan'
    )
    workflow = relationship('WorkflowHandle', back_populates='groups')


class UploadFile(FileHandle):
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
    group = relationship('GroupHandle', back_populates='uploads')


# Workflow Run ----------------------------------------------------------------

"""Workflow runs maintain the run status, the provided argument values for
workflow parameters, and timestamps.
"""


class RunHandle(Base):
    """ Workflow runs may be triggered by workflow group members or they
    represent post-processing workflows. In the latter case the group
    identifier is None.
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'workflow_run'

    run_id = Column(
        String(32),
        primary_key=True
    )
    workflow_id = Column(
        String(32),
        ForeignKey('workflow_template.workflow_id')
    )
    group_id = Column(
        String(32),
        ForeignKey('workflow_group.group_id'),
        nullable=True
    )
    state_type = Column(String(8), nullable=False)
    created_at = Column(String(32), default=util.utc_now, nullable=False)
    started_at = Column(String(32))
    ended_at = Column(String(32))
    arguments = Column(JsonObject)
    result = Column(JsonObject)

    # -- Internal variables ---------------------------------------------------
    _rundir = None

    # -- Relationships --------------------------------------------------------
    files = relationship('RunFile', cascade='all, delete, delete-orphan')
    group = relationship('GroupHandle', back_populates='runs')
    log = relationship('RunMessage', cascade='all, delete, delete-orphan')
    workflow = relationship('WorkflowHandle', back_populates='runs')

    def archive(self):
        """Create a gzipped tar file containing all files in the given resource
        set.

        Returns
        -------
        io.BytesIO
        """
        return util.archive_files([(r.filename, r.name) for r in self.files])

    def get_file(self, by_id=None, by_name=None):
        """Get handle for identified file. A file can either be identified by
        the unique identifier or the unique relative path (name). Returns None
        if the file is not found.

        Raises a ValueError if an invalid combination of parameters is given.

        Patameters
        ----------
        relative_path: string
            Relative path to the file in the run directory.

        Returns
        -------
        flowserv.model.base.RunFile
        """
        if by_id is None and by_name is None:
            raise ValueError('invalid argument combination')
        if by_id is not None and by_name is not None:
            raise ValueError('invalid argument combination')
        if by_id is not None:
            for f in self.files:
                if f.file_id == by_id:
                    f.set_filename(os.path.join(self._rundir, f.relative_path))
                    return f
        if by_name is not None:
            for f in self.files:
                if f.name == by_name:
                    f.set_filename(os.path.join(self._rundir, f.relative_path))
                    return f

    def get_rundir(self):
        """Get the path to the base directory for run files.

        Returns
        -------
        string
        """
        assert self._rundir is not None
        return self._rundir

    def is_active(self):
        """A run is in active state if it is either pending or running.

        Returns
        --------
        bool
        """
        return self.state_type in st.ACTIVE_STATES

    def is_canceled(self):
        """Returns True if the run state is of type CANCELED.

        Returns
        -------
        bool
        """
        return self.state_type == st.STATE_CANCELED

    def is_error(self):
        """Returns True if the run state is of type ERROR.

        Returns
        -------
        bool
        """
        return self.state_type == st.STATE_ERROR

    def is_pending(self):
        """Returns True if the run state is of type PENDING.

        Returns
        -------
        bool
        """
        return self.state_type == st.STATE_PENDING

    def is_running(self):
        """Returns True if the run state is of type RUNNING.

        Returns
        -------
        bool
        """
        return self.state_type == st.STATE_RUNNING

    def is_success(self):
        """Returns True if the run state is of type SUCCESS.

        Returns
        -------
        bool
        """
        return self.state_type == st.STATE_SUCCESS

    def outputs(self):
        """Get specification of output file properties. If the workflow
        template does not contain any output file specifications the result
        is None.

        If the run is associated with a group, then the output file
        specification of the associated workflow is returned. If the run is a
        post-processing run the optional output specification in the post-
        processing workflow template is returned.

        Returns
        -------
        list(flowserv.model.template.files.WorkflowOutputFile)
        """
        if self.group_id is not None:
            return self.workflow.outputs
        else:
            outputs = self.workflow.postproc_spec.get('outputs')
            if outputs is not None:
                outputs = [WorkflowOutputFile.from_dict(f) for f in outputs]
            return outputs

    def set_rundir(self, dirname):
        """Set the path to the base directory for run files.

        Parameters
        ----------
        dirname: string
            Path to directory for run files and folders.
        """
        self._rundir = dirname

    def state(self):
        """Get an instance of the workflow state for the given run.

        Returns
        -------
        flowserv.model.workflow.state.WorkflowState
        """
        if self.state_type == st.STATE_PENDING:
            return st.StatePending(created_at=self.created_at)
        elif self.state_type == st.STATE_RUNNING:
            return st.StateRunning(
                created_at=self.created_at,
                started_at=self.started_at
            )
        elif self.state_type == st.STATE_CANCELED:
            return st.StateCanceled(
                created_at=self.created_at,
                started_at=self.started_at,
                stopped_at=self.ended_at,
                messages=[m.message for m in sorted(self.log, key=by_pos)]
            )
        elif self.state_type == st.STATE_ERROR:
            return st.StateError(
                created_at=self.created_at,
                started_at=self.started_at,
                stopped_at=self.ended_at,
                messages=[m.message for m in sorted(self.log, key=by_pos)]
            )
        else:  # self.state_type == st.STATE_SUCCESS:
            return st.StateSuccess(
                created_at=self.created_at,
                started_at=self.started_at,
                finished_at=self.ended_at,
                files=[f.relative_path for f in self.files]
            )


class RunFile(FileHandle):
    """File resources that are created by successful workflow runs."""
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'run_file'

    file_id = Column(
        String(32),
        default=util.get_unique_identifier,
        primary_key=True
    )
    run_id = Column(
        String(32),
        ForeignKey('workflow_run.run_id')
    )
    relative_path = Column(Text, nullable=False)

    UniqueConstraint('run_id', 'name')

    # Relationships -----------------------------------------------------------
    run = relationship('RunHandle', back_populates='files')

    @property
    def name(self):
        """Implement file handle property 'name'."""
        return self.relative_path


class RunMessage(Base):
    """Log for messages created by workflow runs. Primarily used for error
    messages by now.
    """
    # -- Schema ---------------------------------------------------------------
    __tablename__ = 'run_log'

    run_id = Column(
        String(32),
        ForeignKey('workflow_run.run_id'),
        primary_key=True
    )
    pos = Column(Integer, primary_key=True)
    message = Column(Text, nullable=False)

    # Relationships -----------------------------------------------------------
    run = relationship('RunHandle', back_populates='log')


# -- Helper classes and functions ---------------------------------------------

def by_pos(msg):
    """Helper to sort log messages by position."""
    return msg.pos
