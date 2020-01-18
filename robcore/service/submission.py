# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of API methods that access and manipulate benchmark
submissions and their results.
"""

from robcore.view.submission import SubmissionSerializer
from robcore.view.route import UrlFactory

import robcore.core.error as err
import robcore.model.user.auth as res


class SubmissionService(object):
    """API component that provides methods to access benchmark submissions and
    their runs.
    """
    def __init__(self, engine, manager, auth, repo, urls=None, serializer=None):
        """Initialize the internal reference to the submission manager and to
        the route factory.

        Parameters
        ----------
        engine: robcore.model.benchmark.engine.BenchmarkEngine
            Benchmark engine
        manager: robcore.model.submission.SubmissionManager
            Manager for benchmark submissions
        auth: robcore.model.user.auth.Auth
            Implementation of the authorization policy for the API
        repo: robcore.model.template.repo.benchmark.BenchmarkRepository
            Repository to access registered benchmarks
        urls: robcore.view.route.UrlFactory
            Factory for API resource Urls
        serializer: robcore.view.submission.SubmissionSerializer, optional
            Override the default serializer
        """
        self.engine = engine
        self.manager = manager
        self.auth = auth
        self.repo = repo
        self.urls = urls if not urls is None else UrlFactory()
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = SubmissionSerializer(self.urls)

    def authorize_member(self, submission_id, user):
        """Ensure that the user is a member of the given submission if the
        submission exists. A unauthorized error is raised if the submission
        exists and the user is not a member of the submission. A unknown
        submission error is raised if submission does not exist.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user: robcore.model.user.base.UserHandle
            Handle for user that is requesting access

        Raises
        ------
        robcore.core.error.UnauthorizedAccessError
        robcore.core.error.UnknownSubmissionError
        """
        if not self.auth.is_submission_member(submission_id=submission_id, user=user):
            # At this point it is not clear whether the user is not a member of
            # an existing submission or if the submission does not exist. The
            # attempt to load the submission will fail if the submission does
            # not exist.
            self.manager.get_submission(submission_id, load_members=False)
            raise err.UnauthorizedAccessError()

    def create_submission(
        self, benchmark_id, name, user, parameters=None, members=None
    ):
        """Create a new submission for a given benchmark. Each submission for
        the benchmark has a unique name, a submission owner, and a list of
        additional submission members.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        name: string
            Unique team name
        user: robcore.model.user.base.UserHandle
            Handle for the submission owner
        parameters: dict(string:robcore.model.template.parameter.base.TemplateParameter), optional
            Workflow template parameter declarations
        members: list(string), optional
            List of user identifier for submission members

        Returns
        -------
        dict

        Raises
        ------
        robcore.core.error.ConstraintViolationError
        robcore.core.error.UnknownBenchmarkError
        """
        # Get the template for the given benchmark. This will raise an exception
        # if the benchmark is unknown.
        benchmark = self.repo.get_benchmark(benchmark_id)
        # If additional parameters are given we need to modify the workflow
        # specification accordingly. This may raise an error if a given
        # parameter identifier is not unique.
        template = benchmark.get_template()
        workflow_spec = template.workflow_spec
        if not parameters is None:
            workflow_spec, parameters = self.engine.backend.modify_template(
                workflow_spec=workflow_spec,
                tmpl_parameters=template.parameters,
                add_parameters=parameters
            )
        else:
            parameters = template.parameters
        submission = self.manager.create_submission(
            benchmark_id=benchmark_id,
            name=name,
            user_id=user.identifier,
            parameters=parameters,
            workflow_spec=workflow_spec,
            members=members
        )
        return self.serialize.submission_handle(submission=submission)

    def delete_file(self, submission_id, file_id, user):
        """Delete file with given identifier that was previously uploaded.

        Raises errors if the file or the team is unknown or the user is not
        authorized to delete the file.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file_id: string
            Unique file identifier
        user: robcore.model.user.base.UserHandle
            Handle for user that requested the deletion

        Raises
        ------
        robcore.core.error.UnauthorizedAccessError
        robcore.core.error.UnknownFileError
        """
        # Raise an error if the user does not have delete rights for the
        # submission or if the submission does not exist.
        self.authorize_member(submission_id=submission_id, user=user)
        self.manager.delete_file(submission_id=submission_id, file_id=file_id)

    def delete_submission(self, submission_id, user):
        """Get a given submission and all associated runs and results. If the
        user is not an administrator or a member of the submission an
        unauthorized access error is raised.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user: robcore.model.user.base.UserHandle
            Handle for user that is deleting the submission

        Raises
        ------
        robcore.core.error.UnauthorizedAccessError
        robcore.core.error.UnknownSubmissionError
        """
        # Raise an error if the user is not authorized to delete the submission
        # or if the submission does not exist
        self.authorize_member(submission_id=submission_id, user=user)
        self.manager.delete_submission(submission_id)

    def get_file(self, submission_id, file_id, user):
        """Get handle for file with given identifier that was uploaded by the
        team.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file_id: string
            Unique file identifier
        user: robcore.model.user.base.UserHandle
            Handle for user that is accessing the file

        Returns
        -------
        robcore.core.files.FileHandle, dict

        Raises
        ------
        robcore.core.error.UnauthorizedAccessError
        robcore.core.error.UnknownFileError
        robcore.core.error.UnknownSubmissionError
        """
        # Raise an error if the user does not have  access rights for the
        # submission files or if the submission does not exist.
        self.authorize_member(submission_id=submission_id, user=user)
        # Return the file handle and a serialization of tit
        fh = self.manager.get_file(submission_id=submission_id, file_id=file_id)
        doc = self.serialize.file_handle(submission_id=submission_id, fh=fh)
        return fh, doc

    def get_benchmark(self, benchmark_id):
        """Get descriptor for the benchmark with the given identifier. Raises
        an error if no benchmark with the identifier exists.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier

        Returns
        -------
        robcore.model.template.benchmark.BenchmarkHandle

        Raises
        ------
        robcore.core.error.UnknownBenchmarkError
        """
        return self.repo.get_benchmark(benchmark_id)

    def get_submission(self, submission_id):
        """Get handle for submission with the given identifier.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        dict

        Raises
        ------
        robcore.core.error.UnknownSubmissionError
        """
        submission = self.manager.get_submission(submission_id)
        return self.serialize.submission_handle(submission=submission)

    def list_files(self, submission_id, user):
        """Get a listing of all files that have been uploaded for the given
        submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user: robcore.model.user.base.UserHandle
            Handle for user that requested the deletion

        Returns
        -------
        dict

        Raises
        ------
        robcore.core.error.UnauthorizedAccessError
        robcore.core.error.UnknownSubmissionError
        """
        # Raise an error if the user does not have  access rights for the
        # submission files or if the submission does not exist.
        self.authorize_member(submission_id=submission_id, user=user)
        files = self.manager.list_files(submission_id)
        return self.serialize.file_listing(
            submission_id=submission_id,
            files=files
        )

    def list_submissions(self, benchmark_id=None, user=None):
        """Get a listing of all submissions. If the user handle is given the
        result contains only those submissions that the user is a member of.
        If the benchmark identifier is given the result contains submissions
        only for the given benchmark.

        Parameters
        ----------
        benchmark_id: string, optional
            Unique benchmark identifier
        user: robcore.model.user.base.UserHandle, optional
            Handle for user that is requesting the submission listing

        Returns
        -------
        dict
        """
        if not user is None:
            user_id = user.identifier
        else:
            user_id = None
        submissions = self.manager.list_submissions(
            benchmark_id=benchmark_id,
            user_id=user_id
        )
        return self.serialize.submission_listing(submissions)

    def update_submission(self, submission_id, user, name=None, members=None):
        """Update the name for the team with the given identifier. The access
        token is optional to allow a super user to change team names from the
        command line interface without the need to be a team owner. A web
        service implementation should always ensure that an access token is
        given.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user: robcore.model.user.base.UserHandle
            Handle for user that is accessing the submission
        name: string, optional
            New submission name
        members: list(string), optional
            Modified list of team members

        Returns
        -------
        dict

        Raises
        ------
        robcore.core.error.ConstraintViolationError
        robcore.core.error.UnauthorizedAccessError
        robcore.core.error.UnknownSubmissionError
        """
        # Raise an error if the user does not have  access rights for the
        # submission or if the submission does not exist.
        self.authorize_member(submission_id=submission_id, user=user)
        submission = self.manager.update_submission(
            submission_id,
            name=name,
            members=members
        )
        return self.serialize.submission_handle(submission=submission)

    def upload_file(self, submission_id, file, file_name, user, file_type=None):
        """Create a file for a given submission.

        Parameters
        ----------
        file: werkzeug.datastructures.FileStorage
            File object (e.g., uploaded via HTTP request)
        file_name: string
            Name of the file
        submission_id: string
            Unique submission identifier
        user: robcore.model.user.base.UserHandle
            Handle for user that is uploading the file
        file_type: string, optional
            Identifier for the file type (e.g., the file MimeType). This could
            also by the identifier of a content handler.

        Returns
        -------
        dict

        Raises
        ------
        robcore.core.error.ConstraintViolationError
        robcore.core.error.UnauthorizedAccessError
        robcore.core.error.UnknownSubmissionError
        """
        # Raise an error if the user does not have  access rights for the
        # submission files or if the submission does not exist.
        self.authorize_member(submission_id=submission_id, user=user)
        # Return serialization of the uploaded file
        fh = self.manager.upload_file(
            submission_id=submission_id,
            file=file,
            file_name=file_name,
            file_type=file_type
        )
        return self.serialize.file_handle(submission_id=submission_id, fh=fh)
