# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of API methods that execute, access, and manipulate benchmark
runs.
"""

from robcore.api.serialize.run import RunSerializer
from robcore.api.route import UrlFactory
from robcore.io.files import InputFile
from robcore.model.template.parameter.value import TemplateArgument

import robcore.api.serialize.labels as labels
import robcore.error as err
import robcore.model.user.auth as res
import robcore.util as util


class RunService(object):
    """API component that provides methods to start, access, and manipulate
    benchmark runs.
    """
    def __init__(self, engine, submissions, repo, auth, urls=None, serializer=None):
        """Initialize the internal reference to the benchmark engine, the
        manager for benchmark submissions, the benchmark repository, and to the
        route factory.

        Parameters
        ----------
        engine: robcore.model.benchmark.engine.BenchmarkEngine
            Benchmark engine
        submissions: robcore.model.submission.SubmissionManager
            Manager for benchmark submissions
        repo: robcore.model.template.repo.benchmark.BenchmarkRepository
            Repository to access registered benchmarks
        auth: robcore.model.user.auth.Auth
            Implementation of the authorization policy for the API
        urls: robcore.api.route.UrlFactory
            Factory for API resource Urls
        serializer: robcore.api.serialize.submission.SubmissionSerializer, optional
            Override the default serializer
        """
        self.engine = engine
        self.submissions = submissions
        self.repo = repo
        self.auth = auth
        self.urls = urls if not urls is None else UrlFactory()
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = RunSerializer(self.urls)

    def cancel_run(self, run_id, user):
        """Cancel the run with the given identifier. Returns a serialization of
        the handle for the canceled run.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to cancel the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user: robcore.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.UnauthorizedAccessError
        robcore.error.UnknownRunError
        robcore.error.InvalidRunStateError
        """
        # Ensure that the user has sufficient access rights to cancel the run
        if not self.auth.is_submission_member(run_id=run_id, user=user):
            raise err.UnauthorizedAccessError()
        self.engine.cancel_run(run_id)
        return self.get_run(run_id=run_id, user=user)

    def delete_run(self, run_id, user):
        """Delete the run with the given identifier. Returns a serialization of
        the list of remaining runs for the run's submision.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to delete the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user: robcore.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.UnauthorizedAccessError
        robcore.error.UnknownRunError
        robcore.error.InvalidRunStateError
        """
        # Ensure that the user has sufficient access rights to delete the run
        if not self.auth.is_submission_member(run_id=run_id, user=user):
            raise err.UnauthorizedAccessError()
        run = self.engine.delete_run(run_id)
        return self.list_runs(submission_id=run.submission_id, user=user)

    def get_run(self, run_id, user):
        """Get handle for the given run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user: robcore.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.UnauthorizedAccessError
        robcore.error.UnknownRunError
        """
        # Ensure that the user has read access for the run
        if not self.auth.is_submission_member(run_id=run_id, user=user):
            raise err.UnauthorizedAccessError()
        run = self.engine.get_run(run_id)
        return self.serialize.run_handle(run)

    def list_runs(self, submission_id, user):
        """Get a listing of all run handles for the given submission.

        Raises an unauthorized access error if the user does not have read
        access to the submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user: robcore.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.UnauthorizedAccessError
        robcore.error.UnknownSubmissionError
        """
        # Ensure that the user has read access for submission runs
        if not self.auth.is_submission_member(submission_id=submission_id, user=user):
            raise err.UnauthorizedAccessError()
        return self.serialize.run_listing(
            runs=self.submissions.get_runs(submission_id),
            submission_id=submission_id
        )

    def start_run(self, submission_id, arguments, user):
        """Start a new workflow run for the given submission. The user provided
        arguments are expected to be a list of (key,value)-pairs. The key value
        identifies the template parameter. The of the value depends on the type
        of the parameter.

        Returns a serialization of the handle for the started run.

        Raises an unauthorized access error if the user does not have the
        necessary access to modify the submission.

        Parameters
        ----------
        submission_id: string
            Unique run identifier
        arguments: list(dict)
        user: robcore.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        robcore.error.InvalidArgumentError
        robcore.error.MissingArgumentError
        robcore.error.UnauthorizedAccessError
        robcore.error.UnknownFileError
        robcore.error.UnknownParameterError
        robcore.error.UnknownSubmissionError
        """
        # Ensure that the user has sufficient access rights to create a new run
        if not self.auth.is_submission_member(submission_id=submission_id, user=user):
            raise err.UnauthorizedAccessError()
        # Get the submission handle. This will raise an error if the submission
        # is unknown.
        submission = self.submissions.get_submission(submission_id)
        # Get the workflow template from the handle of the benchmark that the
        # submission belongs to.
        benchmark = self.repo.get_benchmark(submission.benchmark_id)
        template = benchmark.get_template()
        # Create instances of the template arguments from the given list of
        # values. At this point we only distinguish between scalar values and
        # input files. Arguments of type record and list have to be added in a
        # later version.
        run_args = dict()
        for arg in arguments:
            # Validate the given argument
            try:
                util.validate_doc(
                    doc=arg,
                    mandatory_labels=[labels.ID, labels.VALUE],
                    optional_labels=[labels.AS]
                )
            except ValueError as ex:
                raise err.InvalidArgumentError(str(ex))
            arg_id = arg[labels.ID]
            arg_val = arg[labels.VALUE]
            # Raise an error if multiple values are given for the same argument
            if arg_id in run_args:
                raise err.DuplicateArgumentError(arg_id)
            para = template.get_parameter(arg_id)
            if para is None:
                raise err.UnknownParameterError(arg_id)
            if para.is_file():
                # The argument value is expected to be the identifier of an
                # previously uploaded file. This will raise an exception if the
                # file identifier is unknown
                fh = submission.get_file(arg_val)
                if labels.AS in arg:
                    # Convert the file handle to an input file handle if a
                    # target path is given
                    fh = InputFile(fh, target_path=arg[labels.AS])
                val = TemplateArgument(parameter=para, value=fh, validate=True)
            elif para.is_list() or para.is_record():
                raise err.InvalidArgumentError('nested parameters not supported yet')
            else:
                val = TemplateArgument(
                    parameter=para,
                    value=arg_val,
                    validate=True
                )
            run_args[arg_id] = val
        template.validate_arguments(run_args)
        # Start the run and return the serialized run handle.
        run = self.engine.start_run(
            submission_id=submission_id,
            template=template,
            arguments=run_args
        )
        return self.serialize.run_handle(run)
