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

import tarfile

from io import BytesIO

from flowserv.view.run import RunSerializer
from flowserv.view.route import UrlFactory
from flowserv.core.files import InputFile
from flowserv.model.parameter.value import TemplateArgument

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.view.labels as labels


class RunService(object):
    """API component that provides methods to start, access, and manipulate
    benchmark runs.
    """
    def __init__(
        self, engine, submissions, repo, auth, urls=None, serializer=None
    ):
        """Initialize the internal reference to the benchmark engine, the
        manager for benchmark submissions, the benchmark repository, and to the
        route factory.

        Parameters
        ----------
        engine: flowserv.model.benchmark.engine.BenchmarkEngine
            Benchmark engine
        submissions: flowserv.model.submission.SubmissionManager
            Manager for benchmark submissions
        repo: flowserv.model.template.repo.benchmark.BenchmarkRepository
            Repository to access registered benchmarks
        auth: flowserv.model.user.auth.Auth
            Implementation of the authorization policy for the API
        urls: flowserv.view.route.UrlFactory
            Factory for API resource Urls
        serializer: flowserv.view.submission.SubmissionSerializer, optional
            Override the default serializer
        """
        self.engine = engine
        self.submissions = submissions
        self.repo = repo
        self.auth = auth
        self.urls = urls if urls is not None else UrlFactory()
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = RunSerializer(self.urls)

    def authorize_member(self, user, submission_id=None, run_id=None):
        """Ensure that the user is a member of the submission for a run.
        A unauthorized error is raised if the submission exists and the user is
        not a member of the submission. A unknown submission error is raised if
        submission does not exist.

        Parameters
        ----------
        submission_id: string, optional
            Unique submission identifier
        run_id: string, optional
            Unique run identifier
        user: flowserv.model.user.base.UserHandle
            Handle for user that is requesting access

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        is_member = self.auth.is_submission_member(
            submission_id=submission_id,
            run_id=run_id,
            user=user
        )
        if not is_member:
            # At this point it is not clear whether the user is not a member of
            # an existing submission or if the submission or run does not exist.
            if not run_id is None and not self.engine.exists_run(run_id=run_id):
                raise err.UnknownRunError(run_id)
            elif not submission_id is None:
                # The attempt to load the submission will fail if the
                # submission does not exist.
                self.submissions.get_submission(
                    submission_id=submission_id,
                    load_members=False
                )
            raise err.UnauthorizedAccessError()

    def cancel_run(self, run_id, user, reason=None):
        """Cancel the run with the given identifier. Returns a serialization of
        the handle for the canceled run.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to cancel the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user: flowserv.model.user.base.UserHandle
            User that requested the operation
        reason: string, optional
            Optional text describing the reason for cancelling the run

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.InvalidRunStateError
        """
        # Raise an error if the user does not have rights to cancel the run or
        # if the run does not exist.
        self.authorize_member(run_id=run_id, user=user)
        self.engine.cancel_run(run_id, reason=reason)
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
        user: flowserv.model.user.base.UserHandle
            User that requested the operation

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.InvalidRunStateError
        """
        # Raise an error if the user does not have rights to delete the run or
        # if the run does not exist.
        self.authorize_member(run_id=run_id, user=user)
        self.engine.delete_run(run_id)

    def get_result_archive(self, run_id, user):
        """Get compressed tar-archive containing all result files that were
        generated by a given workflow run. If the run is not in sucess state
        a unknown resource error is raised.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user: flowserv.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.UnknownResourceError
        """
        # Raise an error if the user does not have rights to access the run or
        # if the run does not exist.
        self.authorize_member(run_id=run_id, user=user)
        # Get the run handle. If the run is not in success state raise an
        # unknown run error. The files in the handle are keyed by their unique
        # name. All files are added to an im-memory tar archive.
        run = self.engine.get_run(run_id)
        if not run.is_success():
            raise err.UnknownRunError(run_id)
        file_out = BytesIO()
        tar_handle = tarfile.open(fileobj=file_out, mode='w:gz')
        for f in run.list_resources():
            tar_handle.add(name=f.filename, arcname=f.resource_name)
        tar_handle.close()
        file_out.seek(0)
        return file_out

    def get_result_file(self, run_id, resource_id, user):
        """Get file handle for a resource file that was generated as the result
        of a successful workflow run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        resource_id: string
            Unique resource file identifier
        user: flowserv.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.UnknownResourceError
        """
        # Raise an error if the user does not have rights to access the run or
        # if the run does not exist.
        self.authorize_member(run_id=run_id, user=user)
        # Get the run handle. The files in the handle are keyed by their unique
        # name and not the unique resource identifier that is used in web API
        # requests. We need to find the resource by iterating over the list of
        # available resources.
        run = self.engine.get_run(run_id)
        resource = None
        for r in run.list_resources():
            if r.resource_id == resource_id:
                resource = r
                break
        # Raise error if the resource does not exist
        if resource is None:
            raise err.UnknownResourceError(resource_id)
        # Return file handle for resource file
        return resource.file_handle()

    def get_run(self, run_id, user):
        """Get handle for the given run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user: flowserv.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        """
        # Raise an error if the user does not have rights to access the run or
        # if the run does not exist.
        self.authorize_member(run_id=run_id, user=user)
        run = self.engine.get_run(run_id)
        # Get the workflow template from the handle of the benchmark that the
        # run submission belongs to.
        submission = self.submissions.get_submission(run.submission_id)
        return self.serialize.run_handle(run, submission)

    def list_runs(self, submission_id, user):
        """Get a listing of all run handles for the given submission.

        Raises an unauthorized access error if the user does not have read
        access to the submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user: flowserv.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access the
        # submission runs or if the submission does not exist.
        self.authorize_member(submission_id=submission_id, user=user)
        return self.serialize.run_listing(
            runs=self.submissions.get_runs(submission_id),
            submission_id=submission_id
        )

    def start_run(self, submission_id, arguments, user):
        """Start a new workflow run for the given submission. The user provided
        arguments are expected to be a list of (key,value)-pairs. The key value
        identifies the template parameter. The data type of the value depends
        on the type of the parameter.

        Returns a serialization of the handle for the started run.

        Raises an unauthorized access error if the user does not have the
        necessary access to modify the submission.

        Parameters
        ----------
        submission_id: string
            Unique run identifier
        arguments: list(dict)
            List of user provided arguments for template parameters
        user: flowserv.model.user.base.UserHandle
            User that requested the operation

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.InvalidArgumentError
        flowserv.core.error.MissingArgumentError
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownFileError
        flowserv.core.error.UnknownParameterError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to start new runs for
        # the submission or if the submission does not exist.
        self.authorize_member(submission_id=submission_id, user=user)
        # Get the submission handle. This will raise an error if the submission
        # is unknown.
        submission = self.submissions.get_submission(submission_id)
        # Get the workflow template from the handle of the benchmark that the
        # submission belongs to. Get a modified copy of the template based on
        # the (potentially) modified workflow specification and parameters of
        # the submission.
        benchmark = self.repo.get_benchmark(submission.benchmark_id)
        template = benchmark.get_template(
            workflow_spec=submission.workflow_spec,
            parameters=submission.parameters
        )
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
        # Before we start creating directories and copying files make sure that
        # there are values for all template parameters (either in the arguments
        # dictionary or set as default values)
        template.validate_arguments(run_args)
        # Start the run and return the serialized run handle.
        run = submission.start_run(
            template=template,
            arguments=run_args
        )
        return self.serialize.run_handle(run, submission)
