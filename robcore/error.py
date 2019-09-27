# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Exceptions that are raised by the various components of the reproducible
open benchmark platform.
"""


class ROBError(Exception):
    """Base exception indicating that a component of the reproducible open
    benchmark platform encountered an error situation.
    """
    def __init__(self, message):
        """Initialize error message.

        Parameters
        ----------
        message : string
            Error message
        """
        Exception.__init__(self)
        self.message = message

    def __str__(self):
        """Get printable representation of the exception.

        Returns
        -------
        string
        """
        return self.message


class UnknownObjectError(ROBError):
    """Generic error for references to unknown objects."""
    def __init__(self, obj_id, type_name='object'):
        """Initialize error message.

        Parameters
        ----------
        obj_id: string
            Unique object identifier
        type_name: string, optional
            Name of type of the referenced object
        """
        super(UnknownObjectError, self).__init__(
            message='unknown {} \'{}\''.format(type_name, obj_id)
        )


# -- Authentication and Authorization errors -----------------------------------

class UnauthenticatedAccessError(ROBError):
    """This exception is raised if an unauthenticated user attempts to access
    or manipulate application resources.
    """
    def __init__(self):
        """Initialize the default error message."""
        super(UnauthenticatedAccessError, self).__init__(
            message='not logged in'
        )


class UnauthorizedAccessError(ROBError):
    """This exception is raised if an authenticated user attempts to access
    or manipulate application resources that they have not authorization to.
    """
    def __init__(self):
        """Initialize the default error message."""
        super(UnauthorizedAccessError, self).__init__(
            message='not authorized'
        )


# -- Configuration -------------------------------------------------------------

class MissingConfigurationError(ROBError):
    """Error indicating that the value for a mandatory environment variable is
    not set.
    """
    def __init__(self, var_name):
        """Initialize error message.

        Parameters
        ----------
        var_name: string
            Environment variable name
        """
        super(MissingConfigurationError, self).__init__(
            message='variable \'{}\' not set'.format(var_name)
        )


# -- Constraints on argument values --------------------------------------------

class ConstraintViolationError(ROBError):
    """Exception raised when an (implicit) constraint is violated by a requested
    operation. Example constraints are (i) names that are expected to be
    unique, (ii) names that cannot have more than n characters long, etc.
    """
    def __init__(self, message):
        """Initialize error message.

        Parameters
        ----------
        message : string
            Error message
        """
        super(ConstraintViolationError, self).__init__(message=message)


class DuplicateRunError(ROBError):
    """Exception indicating that a given run identifier is not unique.
    """
    def __init__(self, identifier):
        """Initialize error message for duplicate run identifier.

        Parameters
        ----------
        identifier: string
            Unique run identifier
        """
        super(DuplicateRunError, self).__init__(
            message='non-unique run identifier \'{}\''.format(identifier)
        )


class DuplicateUserError(ROBError):
    """Exception indicating that a given user already exists."""
    def __init__(self, user_id):
        """Initialize error message.

        Parameters
        ----------
        user_id : string
            Unique user identifier
        """
        super(DuplicateUserError, self).__init__(
            message='duplicate user \'{}\''.format(user_id)
        )


class InvalidParameterError(ROBError):
    """Exception indicating that a given template parameter is invalid.
    """
    def __init__(self, message):
        """Initialize error message. The message for invalid parameter errors
        is depending on the context

        Parameters
        ----------
        message : string
            Error message
        """
        super(InvalidParameterError, self).__init__(message=message)


class InvalidRunStateError(ROBError):
    """Exception indicating that an attempt to modify the state of a run was
    made that is not allowed in the current run state or that would result in
    an illegal sequence of workflow states.
    """
    def __init__(self, state, resulting_state=None):
        """Initialize the error message.

        Parameters
        ----------
        state: robcore.model.workflow.state.WorkflowState
            Current run state
        resulting_state: robcore.model.workflow.state.WorkflowState, optional
            Resulting workflow state (for invalid state sequence)
        """
        if resulting_state is None:
            msg = 'invalid operation for run in state {}'.format(state)
        else:
            msg = 'illegal state transition from {} to {}'
            msg = msg.format(state, resulting_state)
        super(InvalidRunStateError, self).__init__(message=msg)


class InvalidTemplateError(ROBError):
    """Exception indicating that a given workflow template is invalid or has
    missing elements.
    """
    def __init__(self, message):
        """Initialize error message. The message for invalid template errors
        is depending on the context

        Parameters
        ----------
        message : string
            Error message
        """
        super(InvalidTemplateError, self).__init__(message=message)


class MissingArgumentError(ROBError):
    """Exception indicating that a required parameter in a workflow template
    has no argument given for a workflow run.
    """
    def __init__(self, identifier):
        """Initialize missing argument error message for parameter identifier.

        Parameters
        ----------
        identifier: string
            Unique parameter identifier
        """
        super(MissingArgumentError, self).__init__(
            message='missing argument for \'{}\''.format(identifier)
        )


# -- Unknown resources ---------------------------------------------------------

class UnknownBenchmarkError(UnknownObjectError):
    """Exception indicating that a given benchmark identifier is unknown."""
    def __init__(self, benchmark_id):
        """Initialize error message.

        Parameters
        ----------
        benchmark_id : string
            Unique benchmark identifier
        """
        super(UnknownBenchmarkError, self).__init__(
            obj_id=benchmark_id,
            type_name='benchmark'
        )


class UnknownFileError(UnknownObjectError):
    """Exception indicating that a given file identifier is unknown."""
    def __init__(self, file_id):
        """Initialize error message.

        Parameters
        ----------
        file_id : string
            Unique file identifier
        """
        super(UnknownFileError, self).__init__(
            obj_id=file_id,
            type_name='file'
        )


class UnknownParameterError(UnknownObjectError):
    """Exception indicating that a workflow specification references a parameter
    that is not defined for a given template.
    """
    def __init__(self, identifier):
        """Initialize error message for given parameter identifier.

        Parameters
        ----------
        identifier: string
            Unique template parameter identifier
        """
        super(UnknownParameterError, self).__init__(
            type_name='parameter',
            obj_id=identifier
        )


class UnknownRequestError(UnknownObjectError):
    """Exception indicating that a given password reset request identifier is
    unknown.
    """
    def __init__(self, request_id):
        """Initialize error message.

        Parameters
        ----------
        request_id : string
            Unique reset request identifier
        """
        super(UnknownRequestError, self).__init__(
            obj_id=request_id,
            type_name='request'
        )


class UnknownRunError(UnknownObjectError):
    """Exception indicating that a given run identifier does not reference a
    known workflow run.
    """
    def __init__(self, identifier):
        """Initialize error message for unknown run identifier.

        Parameters
        ----------
        identifier: string
            Unique run identifier
        """
        super(UnknownRunError, self).__init__(
            type_name='run',
            obj_id=identifier
        )


class UnknownSubmissionError(UnknownObjectError):
    """Exception indicating that a given submission identifier is unknown."""
    def __init__(self, user_id):
        """Initialize error message.

        Parameters
        ----------
        user_id : string
            Unique user identifier
        """
        super(UnknownSubmissionError, self).__init__(
            obj_id=user_id,
            type_name='submission'
        )


class UnknownTemplateError(UnknownObjectError):
    """Error when referencing a workflow template with an unknown identifier.
    """
    def __init__(self, identifier):
        """Initialize error message.

        Parameters
        ----------
        identifier: string
            Unique template identifier
        """
        super(UnknownTemplateError, self).__init__(
            type_name='template',
            obj_id=identifier
        )


class UnknownUserError(UnknownObjectError):
    """Exception indicating that a given user identifier is unknown."""
    def __init__(self, user_id):
        """Initialize error message.

        Parameters
        ----------
        user_id : string
            Unique user identifier
        """
        super(UnknownUserError, self).__init__(
            obj_id=user_id,
            type_name='user'
        )
