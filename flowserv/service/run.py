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

import logging
import shutil

from flowserv.core.files import FileHandle, InputFile
from flowserv.model.parameter.value import TemplateArgument
from flowserv.model.run.base import RunHandle
from flowserv.model.template.base import WorkflowTemplate

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.template.base as tmpl
import flowserv.service.postproc.base as postbase
import flowserv.service.postproc.util as postutil


"""Labels for start run request bodies."""
ARG_AS = 'as'
ARG_ID = 'id'
ARG_VALUE = 'value'


class RunService(object):
    """API component that provides methods to start, access, and manipulate
    workflow runs and their resources.
    """
    def __init__(
        self, run_manager, group_manager, workflow_repo, ranking_manager,
        backend, auth, serializer
    ):
        """Initialize the internal reference to the workflow controller, the
        runa and group managers, and to the serializer.

        Parameters
        ----------
        run_manager: flowserv.model.run.manager.RunManager
            Manager for workflow runs
        group_manager: flowserv.model.group.manager.GroupManager
            Manager for workflow groups
        workflow_repo: flowserv.model.workflow.repo.WorkflowRepository
            Repository for workflow templates
        ranking_manager: flowserv.model.ranking.manager.RankingManager
            Manager for workflow evaluation rankings
        backend: flowserv.controller.base.WorkflowController
            Workflow engine controller
        auth: flowserv.model.user.auth.Auth
            Implementation of the authorization policy for the API
        serializer: flowserv.view.run.RunSerializer
            Override the default serializer
        """
        self.run_manager = run_manager
        self.group_manager = group_manager
        self.workflow_repo = workflow_repo
        self.ranking_manager = ranking_manager
        self.backend = backend
        self.auth = auth
        self.serialize = serializer

    def cancel_run(self, run_id, user_id, reason=None):
        """Cancel the run with the given identifier. Returns a serialization of
        the handle for the canceled run.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to cancel the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user_id: string
            Unique user identifier
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
        if not self.auth.is_group_member(run_id=run_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
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
        return self.serialize.run_handle(
            run=run.update_state(state),
            group=self.group_manager.get_group(run.group_id)
        )

    def delete_run(self, run_id, user_id):
        """Delete the run with the given identifier.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to delete the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user_id: string
            Unique user identifier

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.InvalidRunStateError
        """
        # Raise an error if the user does not have rights to delete the run or
        # if the run does not exist.
        if not self.auth.is_group_member(run_id=run_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        # Get the handle for the run. Raise an error if the run is still
        # in an active state.
        run = self.run_manager.get_run(run_id)
        if run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Use the run manager to delete the run from the underlying database
        # and to delete all run files
        self.run_manager.delete_run(run_id)

    def get_result_archive(self, run_id, user_id=None):
        """Get compressed tar-archive containing all result files that were
        generated by a given workflow run. If the run is not in sucess state
        a unknown resource error is raised.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user_id: string, optional
            Unique user identifier

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.UnknownResourceError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow run or if the run does not exist (only if the user
        # identifier is given).
        if user_id is not None:
            is_member = self.auth.is_group_member(
                run_id=run_id,
                user_id=user_id
            )
            if not is_member:
                raise err.UnauthorizedAccessError()
        # Get the run handle. If the run is not in success state raise an
        # unknown run error. The files in the handle are keyed by their unique
        # name. All files are added to an im-memory tar archive.
        run = self.run_manager.get_run(run_id)
        if not run.is_success():
            raise err.UnknownRunError(run_id)
        return run.resources.targz()

    def get_result_file(self, run_id, resource_id, user_id=None):
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
        user_id: string, optional
            Unique user identifier

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownRunError
        flowserv.core.error.UnknownResourceError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow run or if the run does not exist (only if the user
        # identifier is given).
        if user_id is not None:
            is_member = self.auth.is_group_member(
                run_id=run_id,
                user_id=user_id
            )
            if not is_member:
                raise err.UnauthorizedAccessError()
        # Get the run handle to retrieve the resource. Raise error if the
        # resource does not exist
        run = self.run_manager.get_run(run_id)
        resource = run.resources.get_resource(identifier=resource_id)
        if resource is None:
            raise err.UnknownResourceError(resource_id)
        # Return file handle for resource file
        return resource

    def get_run(self, run_id, user_id):
        """Get handle for the given run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        user_id: string
            Unique user identifier

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
        if not self.auth.is_group_member(run_id=run_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        # Get the run and the workflow group it belongs to. The group is needed
        # to serialize the result.
        run = self.run_manager.get_run(run_id)
        return self.serialize.run_handle(
            run=run,
            group=self.group_manager.get_group(run.group_id)
        )

    def list_runs(self, group_id, user_id):
        """Get a listing of all run handles for the given workflow group.

        Raises an unauthorized access error if the user does not have read
        access to the workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        user_id: string
            Unique user identifier

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
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        return self.serialize.run_listing(
            runs=self.run_manager.list_runs(group_id=group_id),
            group_id=group_id
        )

    def poll_runs(self, group_id, user_id, state=None):
        """Get list of identifier for group runs that are currently in the
        given state. By default, the active runs are returned.

        Raises an unauthorized access error if the user does not have read
        access to the workflow group.

        Parameters
        ----------
        group_id: string, optional
            Unique workflow group identifier
        user_id: string
            Unique user identifier
        state: string, Optional
                State identifier query

        Returns
        -------
        list(string)

        Raises
        ------
        flowserv.core.error.UnauthorizedAccessError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access the
        # workflow group runs or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        runs = self.run_manager.poll_runs(group_id=group_id, state=state)
        return self.serialize.runid_listing(runs)

    def start_run(self, group_id, arguments, user_id):
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
        user_id: string
            Unique user identifier

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
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
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
                    mandatory=[ARG_ID, ARG_VALUE],
                    optional=[ARG_AS]
                )
            except ValueError as ex:
                raise err.InvalidArgumentError(str(ex))
            arg_id = arg[ARG_ID]
            arg_val = arg[ARG_VALUE]
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
                if ARG_AS in arg:
                    # Convert the file handle to an input file handle if a
                    # target path is given
                    fh = InputFile(fh, target_path=arg[ARG_AS])
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
            arguments=run_args
        )
        run_id = run.identifier
        # Execute the benchmark workflow for the given set of arguments.
        state = self.backend.exec_workflow(
            run=run,
            template=template,
            arguments=run_args
        )
        # Update the run state if it is no longer pending for execution. Make
        # sure to call the update run method for the server to ensure that
        # results are inserted and post-processing workflows started.
        if not state.is_pending():
            self.update_run(
                run_id=run_id,
                state=state
            )
        run = RunHandle(
            identifier=run_id,
            workflow_id=workflow.identifier,
            group_id=group_id,
            state=state,
            arguments=run.arguments,
            rundir=run.rundir
        )
        return self.serialize.run_handle(run, group)

    def update_run(self, run_id, state, commit_changes=True):
        """Update the state of the given run. For runs that are in a SUCCESS
        state the workflow evaluation ranking is updated (if a result schema
        is defined for the corresponding template). If the ranking results
        change, an optional post-processing step is executed (synchronously).
        These changes occur before the state of the workflow is updated in the
        underlying database

        Parameters
        ----------
        run_id: string
            Unique identifier for the run
        state: robcore.model.workflow.state.WorkflowState
            New workflow state
        commit_changes: bool, optional
            Commit all changes to the database if true

        Raises
        ------
        robcore.core.error.ConstraintViolationError
        """
        # We give special attention to runs that are in SUCCESS state.
        if state.is_success():
            # Get the current hanlde for the run to have access to the
            # associated workflow
            run = self.run_manager.get_run(run_id)
            # Get the template and the result schema for the workflow that the
            # run belongs to.
            workflow = self.workflow_repo.get_workflow(run.workflow_id)
            template = workflow.get_template()
            result_schema = template.get_schema()
            # Insert result values into the ranking if the template defines a
            # result schema.
            if result_schema is not None:
                self.ranking_manager.insert_result(
                    workflow_id=run.workflow_id,
                    run_id=run_id,
                    result_schema=result_schema,
                    resources=state.resources,
                    commit_changes=False
                )
            # After the results have been successfully added to the ranking
            # we commit the new run state to the database
            self.run_manager.update_run(run_id=run_id, state=state)
            # Execute a post-processing workflow if it is specified in the
            # template and if the ranking results have changed.
            postproc_spec = template.postproc_spec
            if result_schema is not None and postproc_spec is not None:
                # Get the latest ranking for the workflow and create a
                # sorted list of run identifier to compare agains the
                # current post-processing key for the workflow.
                ranking = self.ranking_manager.get_ranking(
                    workflow_id=run.workflow_id,
                    result_schema=result_schema
                )
                runs = sorted([r.run_id for r in ranking])
                # Run post-processing task synchronously if the current
                # post-processing resources where generated for a different
                # set of runs than those in the ranking.
                if runs != workflow.get_postproc_key():
                    msg = 'Run post-processing workflow for {}'
                    logging.debug(msg.format(workflow.identifier))
                    run_postproc_workflow(
                        postproc_spec=postproc_spec,
                        workflow=workflow,
                        ranking=ranking,
                        runs=runs,
                        run_manager=self.run_manager,
                        backend=self.backend
                    )
        else:
            self.run_manager.update_run(run_id=run_id, state=state)


# -- Helper functions ---------------------------------------------------------

def run_postproc_workflow(
    postproc_spec, workflow, ranking, runs, run_manager, backend
):
    """Run post-processing workflow for a workflow template.
    """
    workflow_spec = postproc_spec.get(tmpl.PPLBL_WORKFLOW)
    pp_inputs = postproc_spec.get(tmpl.PPLBL_INPUTS, {})
    pp_files = pp_inputs.get(tmpl.PPLBL_FILES, [])
    # Prepare temporary directory with result files for all
    # runs in the ranking. The created directory is the only
    # run argument
    strace = None
    try:
        datadir = postutil.prepare_postproc_data(
            input_files=pp_files,
            ranking=ranking,
            run_manager=run_manager
        )
        postproc_arguments = {
            postbase.PARA_RUNS: TemplateArgument(
                parameter=postbase.PARAMETERS[0],
                value=InputFile(
                    f_handle=FileHandle(filename=datadir),
                    target_path=pp_inputs.get(
                        tmpl.PPLBL_RUNS,
                        postbase.RUNS_DIR
                    )
                )
            )
        }
    except (AttributeError, OSError, IOError) as ex:
        logging.error(ex)
        strace = util.stacktrace(ex)
        postproc_arguments = dict()
    # Create a new run for the workflow. The identifier for the run group is
    # None.
    run = run_manager.create_run(
        workflow_id=workflow.identifier,
        group_id=None,
        arguments=postproc_arguments,
        commit_changes=False
    )
    # Set the new run as the current post-processing run and enter the run keys
    workflow.update_postproc(
        run_id=run.identifier,
        runs=runs
    )
    if strace is not None:
        # If there were data preparation errors set the created run into an
        # error state and return.
        run_manager.update_run(
            run_id=run.identifier,
            state=run.state.error(messages=strace)
        )
    else:
        # Execute the post-processing workflow asynchronously if
        # there were no data preparation errors.
        postproc_state = backend.exec_workflow(
            run=run,
            template=WorkflowTemplate(
                workflow_spec=workflow_spec,
                sourcedir=workflow.get_template().sourcedir,
                parameters=postbase.PARAMETERS
            ),
            arguments=postproc_arguments
        )
        # Update the rpost-processing workflow run state if it is
        # no longer pending for execution.
        if not postproc_state.is_pending():
            run_manager.update_run(
                run_id=run.identifier,
                state=postproc_state
            )
        # Remove the temporary input folder
        shutil.rmtree(datadir)
