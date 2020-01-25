# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow run API component provides methods that execute, access, and
manipulate workflow runs and their results.
"""

from flowserv.view.run import RunSerializer
from flowserv.core.files import InputFile
from flowserv.model.parameter.value import TemplateArgument
from flowserv.model.run.base import RunHandle

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.view.labels as labels


class RunService(object):
    """API component that provides methods to start, access, and manipulate
    workflow runs and their resources.
    """
    def __init__(
        self, run_manager, group_manager, workflow_repo, backend, auth, urls,
        serializer=None
    ):
        """Initialize the internal reference to the workflow controller, the
        runa and group managers, and to the route factory.

        Parameters
        ----------
        run_manager: flowserv.model.run.manager.RunManager
            Manager for workflow runs
        group_manager: flowserv.model.group.manager.GroupManager
            Manager for workflow groups
        workflow_repo: flowserv.model.workflow.repo.WorkflowRepository
            Repository for workflow templates
        backend: flowserv.controller.base.WorkflowController
            Workflow engine controller
        auth: flowserv.model.user.auth.Auth
            Implementation of the authorization policy for the API
        urls: flowserv.view.route.UrlFactory
            Factory for API resource Urls
        serializer: flowserv.view.run.RunSerializer, optional
            Override the default serializer
        """
        self.run_manager = run_manager
        self.group_manager = group_manager
        self.workflow_repo = workflow_repo
        self.backend = backend
        self.auth = auth
        self.urls = urls
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = RunSerializer(self.urls)

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
        self.auth.is_group_member(run_id=run_id, user=user)
        # Get the run handle. Raise an error if the run is not in an active
        # state
        run = self.run_manager.get_run(run_id)
        if not run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Cancel execution at the backend
        self.backend.cancel_run(run_id)
        # Update the run state and return the run handle
        messages = None
        if reason is not None:
            messages = list([reason])
        state = run.state.cancel(messages=messages)
        self.run_manager.update_run(
            run_id=run_id,
            state=state
        )
        return run.update_state(state)

    def delete_run(self, run_id, user):
        """Delete the run with the given identifier.

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
        self.auth.is_group_member(run_id=run_id, user=user)
        # Get the handle for the run. Raise an error if the run is still
        # in an active state.
        run = self.run_manager.get_run(run_id)
        if run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Use the run manager to delete the run from the underlying database
        # and to delete all run files
        self.run_manager.delete_run(run_id)

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
        self.auth.is_group_member(run_id=run_id, user=user)
        # Get the run handle. If the run is not in success state raise an
        # unknown run error. The files in the handle are keyed by their unique
        # name. All files are added to an im-memory tar archive.
        run = self.run_manager.get_run(run_id)
        if not run.is_success():
            raise err.UnknownRunError(run_id)
        return util.targzip(run.resources)

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
        # Get the run handle to retrieve the resource. Raise error if the
        # resource does not exist
        run = self.run_manager.get_run(run_id)
        resource = run.resources.get_resource(identifier=resource_id)
        if resource is None:
            raise err.UnknownResourceError(resource_id)
        # Return file handle for resource file
        return resource

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
        self.auth.is_group_member(run_id=run_id, user=user)
        # Get the run and the workflow group it belongs to. The group is needed
        # to serialize the result.
        run = self.run_manager.get_run(run_id)
        return self.serialize.run_handle(
            run=run,
            group=self.group_manager.get_group(run.group_id)
        )

    def list_runs(self, group_id, user):
        """Get a listing of all run handles for the given workflow group.

        Raises an unauthorized access error if the user does not have read
        access to the workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
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
        # workflow group runs or if the workflow group does not exist.
        self.auth.is_group_member(group_id=group_id, user=user)
        return self.serialize.run_listing(
            runs=self.run_manager.list_runs(group_id=group_id),
            group_id=group_id
        )

    def start_run(self, group_id, arguments, user):
        """Start a new workflow run for the given group. The user provided
        arguments are expected to be a list of (key,value)-pairs. The key value
        identifies the template parameter. The data type of the value depends
        on the type of the parameter.

        Returns a serialization of the handle for the started run.

        Raises an unauthorized access error if the user does not have the
        necessary access to modify the workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
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
        # the workflow group or if the workflow group does not exist.
        self.auth.is_group_member(group_id=group_id, user=user)
        # Get handle for the given user group to enable access to uploaded
        # files and the identifier of the associated workflow.
        group = self.group_manager.get_group(group_id)
        # Get the template from the workflow that the workflow group belongs
        # to. Get a modified copy of the template based on  the (potentially)
        # modified workflow specification and parameters of the workflow group.
        workflow = self.workflow_repo.get_workflow(group.workflow_id)
        template = workflow.get_template(
            workflow_spec=group.workflow_spec,
            parameters=group.parameters
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
                fh = group.get_file(arg_val)
                if labels.AS in arg:
                    # Convert the file handle to an input file handle if a
                    # target path is given
                    fh = InputFile(fh, target_path=arg[labels.AS])
                val = TemplateArgument(parameter=para, value=fh, validate=True)
            elif para.is_list() or para.is_record():
                raise err.InvalidArgumentError('unsupported parameter type')
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
        run = self.run_manager.create_run(
            workflow_id=group.workflow_id,
            group_id=group_id,
            arguments=arguments
        )
        run_id = run.identifier
        # Execute the benchmark workflow for the given set of arguments.
        state = self.backend.exec_workflow(
            run_id=run_id,
            template=template,
            arguments=arguments
        )
        # Update the run state if it is no longer pending for execution.
        if not state.is_pending():
            self.run_manager.update_run(
                run_id=run_id,
                state=state
            )
        run = RunHandle(
            identifier=run_id,
            group_id=group_id,
            state=state,
            arguments=run.arguments
        )
        return self.serialize.run_handle(run, group)
