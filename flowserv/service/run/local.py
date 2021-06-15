# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow run API component provides methods that execute, access, and
manipulate workflow runs and their results. The local run service accesses all
resources directly via a the database model.
"""

from typing import Dict, List, Optional

import logging

from flowserv.controller.base import WorkflowController
from flowserv.model.auth import Auth
from flowserv.model.base import WorkflowObject
from flowserv.model.files import FileHandle, run_tmpdir
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.parameter.actor import ActorValue
from flowserv.model.parameter.files import InputDirectory
from flowserv.model.ranking import RankingManager, RunResult
from flowserv.model.run import RunManager
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.state import WorkflowState
from flowserv.service.postproc.base import PARAMETERS, PARA_RUNS, RUNS_DIR, prepare_postproc_data
from flowserv.service.run.argument import deserialize_arg, deserialize_fh, is_fh, serialize_arg
from flowserv.service.run.base import RunService
from flowserv.view.run import RunSerializer
from flowserv.volume.base import StorageVolume

import flowserv.error as err
import flowserv.model.files as dirs
import flowserv.util as util


class LocalRunService(RunService):
    """API component that provides methods to start, access, and manipulate
    workflow runs and their resources. Uses the database model classes to
    access and manipulate the database state.
    """
    def __init__(
        self, run_manager: RunManager, group_manager: WorkflowGroupManager,
        ranking_manager: RankingManager, backend: WorkflowController,
        fs: StorageVolume, auth: Auth, user_id: Optional[str] = None,
        serializer: Optional[RunSerializer] = None
    ):
        """Initialize the internal reference to the workflow controller, the
        runa and group managers, and to the serializer.

        Parameters
        ----------
        run_manager: flowserv.model.run.RunManager
            Manager for workflow runs
        group_manager: flowserv.model.group.WorkflowGroupManager
            Manager for workflow groups
        ranking_manager: flowserv.model.ranking.RankingManager
            Manager for workflow evaluation rankings
        backend: flowserv.controller.base.WorkflowController
            Workflow engine controller
        fs: flowserv.volume.base.StorageVolume
            File store for run input and output files.
        auth: flowserv.model.auth.Auth
            Implementation of the authorization policy for the API
        user_id: string, default=None
            Identifier of an authenticated user.
        serializer: flowserv.view.run.RunSerializer
            Override the default serializer
        """
        self.run_manager = run_manager
        self.group_manager = group_manager
        self.ranking_manager = ranking_manager
        self.backend = backend
        self.fs = fs
        self.auth = auth
        self.user_id = user_id
        self.serialize = serializer if serializer is not None else RunSerializer()

    def cancel_run(self, run_id: str, reason: Optional[str] = None) -> Dict:
        """Cancel the run with the given identifier. Returns a serialization of
        the handle for the canceled run.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to cancel the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        reason: string, optional
            Optional text describing the reason for cancelling the run

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        flowserv.error.InvalidRunStateError
        """
        # Raise an error if the user does not have rights to cancel the run or
        # if the run does not exist.
        if not self.auth.is_group_member(run_id=run_id, user_id=self.user_id):
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
        state = run.state().cancel(messages=messages)
        run = self.run_manager.update_run(run_id=run_id, state=state)
        return self.serialize.run_handle(run=run, group=run.group)

    def delete_run(self, run_id: str) -> Dict:
        """Delete the run with the given identifier.

        Raises an unauthorized access error if the user does not have the
        necessary access rights to delete the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        flowserv.error.InvalidRunStateError
        """
        # Raise an error if the user does not have rights to delete the run or
        # if the run does not exist.
        if not self.auth.is_group_member(run_id=run_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        # Get the handle for the run. Raise an error if the run is still
        # in an active state.
        run = self.run_manager.get_run(run_id)
        if run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Use the run manager to delete the run from the underlying database
        # and to delete all run files
        self.run_manager.delete_run(run_id)

    def get_result_archive(self, run_id: str) -> FileHandle:
        """Get compressed tar-archive containing all result files that were
        generated by a given workflow run. If the run is not in sucess state
        a unknown resource error is raised.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        flowserv.model.files.FileHandle

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        flowserv.error.UnknownFileError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow run or if the run does not exist (only if the user
        # identifier is given).
        if self.user_id is not None:
            is_member = self.auth.is_group_member(
                run_id=run_id,
                user_id=self.user_id
            )
            if not is_member:
                raise err.UnauthorizedAccessError()
        # Get the run handle. If the run is not in success state raise an
        # unknown run error. The files in the handle are keyed by their unique
        # name. All files are added to an im-memory tar archive.
        return self.run_manager.get_runarchive(run_id=run_id)

    def get_result_file(self, run_id: str, file_id: str) -> FileHandle:
        """Get file handle for a resource file that was generated as the result
        of a successful workflow run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier.
        file_id: string
            Unique result file identifier.

        Returns
        -------
        flowserv.model.files.FileHandle

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        flowserv.error.UnknownFileError
        """
        # Raise an error if the user does not have rights to access files for
        # the workflow run or if the run does not exist (only if the user
        # identifier is given).
        if self.user_id is not None:
            is_member = self.auth.is_group_member(
                run_id=run_id,
                user_id=self.user_id
            )
            if not is_member:
                raise err.UnauthorizedAccessError()
        # Get the run handle to retrieve the resource. Raise error if the
        # resource does not exist
        return self.run_manager.get_runfile(run_id=run_id, file_id=file_id)

    def get_run(self, run_id: str) -> Dict:
        """Get handle for the given run.

        Raises an unauthorized access error if the user does not have read
        access to the run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownRunError
        """
        # Raise an error if the user does not have rights to access the run or
        # if the run does not exist.
        if not self.auth.is_group_member(run_id=run_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        # Get the run and the workflow group it belongs to. The group is needed
        # to serialize the result.
        run = self.run_manager.get_run(run_id)
        return self.serialize.run_handle(run=run, group=run.group)

    def list_runs(self, group_id: str, state: Optional[str] = None):
        """Get a listing of all run handles for the given workflow group.

        Raises an unauthorized access error if the user does not have read
        access to the workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to access the
        # workflow group runs or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        return self.serialize.run_listing(
            runs=self.run_manager.list_runs(group_id=group_id, state=state)
        )

    def start_run(
        self, group_id: str, arguments: List[Dict], config: Optional[Dict] = None
    ) -> Dict:
        """Start a new workflow run for the given group. The user provided
        arguments are expected to be a list of (name,value)-pairs. The name
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
            List of user provided arguments for template parameters.
        config: dict, default=None
            Optional implementation-specific configuration settings that can be
            used to overwrite settings that were initialized at object creation.

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.InvalidArgumentError
        flowserv.error.MissingArgumentError
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownFileError
        flowserv.error.UnknownParameterError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to start new runs for
        # the workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        # Get handle for the given user group to enable access to uploaded
        # files and the identifier of the associated workflow.
        group = self.group_manager.get_group(group_id)
        # Get the template from the workflow that the workflow group belongs
        # to. Get a modified copy of the template based on  the (potentially)
        # modified workflow specification and parameters of the workflow group.
        template = group.workflow.get_template(
            workflow_spec=group.workflow_spec,
            parameters=group.parameters
        )
        # Create instances of the template arguments from the given list of
        # values. At this point we only distinguish between scalar values and
        # input files. Also create a mapping from he argument list that is used
        # stored in the database.
        run_args = dict()
        serialized_args = list()
        for arg in arguments:
            arg_id, arg_val = deserialize_arg(arg)
            # Raise an error if multiple values are given for the same argument
            if arg_id in run_args:
                raise err.DuplicateArgumentError(arg_id)
            para = template.parameters.get(arg_id)
            if para is None:
                raise err.UnknownParameterError(arg_id)
            if is_fh(arg_val):
                file_id, target = deserialize_fh(arg_val)
                # The argument value is expected to be the identifier of an
                # previously uploaded file. This will raise an exception if the
                # file identifier is unknown.
                fileobj = self.group_manager.get_uploaded_file(
                    group_id=group_id,
                    file_id=file_id
                ).fileobj
                run_args[arg_id] = para.cast(value=(fileobj, target))
            else:
                run_args[arg_id] = para.cast(arg_val)
            # Actor values as parameter values canno be serialized. for now,
            # we only store the serialized workflow step but no information
            # about the additional input files.
            if isinstance(arg_val, ActorValue):
                arg_val = arg_val.spec
            serialized_args.append(serialize_arg(name=arg_id, value=arg_val))
        # Before we start creating directories and copying files make sure that
        # there are values for all template parameters (either in the arguments
        # dictionary or set as default values)
        template.validate_arguments(run_args)
        # Start the run.
        run = self.run_manager.create_run(
            group=group,
            arguments=serialized_args
        )
        run_id = run.run_id
        # Use default engine configuration if the configuration argument was
        # not given.
        config = config if config else group.engine_config
        staticdir = dirs.workflow_staticdir(group.workflow.workflow_id)
        state, runstore = self.backend.exec_workflow(
            run=run,
            template=template,
            arguments=run_args,
            staticfs=self.fs.get_store_for_folder(key=staticdir),
            config=config
        )
        # Update the run state if it is no longer pending for execution. Make
        # sure to call the update run method for the server to ensure that
        # results are inserted and post-processing workflows started.
        if not state.is_pending():
            self.update_run(
                run_id=run_id,
                state=state,
                runstore=runstore
            )
            return self.get_run(run_id)
        return self.serialize.run_handle(run, group)

    def update_run(
        self, run_id: str, state: WorkflowState,
        runstore: Optional[StorageVolume] = None
    ):
        """Update the state of the given run. For runs that are in a SUCCESS
        state the workflow evaluation ranking is updated (if a result schema
        is defined for the corresponding template). If the ranking results
        change, an optional post-processing step is executed (synchronously).
        These changes occur before the state of the workflow is updated in the
        underlying database.

        All run result files are maintained in a engine-specific storage volume
        before being moved to the file storage. For runs that are incative the
        ``runstore`` parameter is expected to reference the run folder.

        Parameters
        ----------
        run_id: string
            Unique identifier for the run
        state: flowserv.model.workflow.state.WorkflowState
            New workflow state.
        runstore: flowserv.volume.base.StorageVolume, default=None
            Storage volume containing the run (result) files for a successful
            workflow run.

        Raises
        ------
        flowserv.error.ConstraintViolationError
        """
        # Commit new run state.
        run = self.run_manager.update_run(
            run_id=run_id,
            state=state,
            runstore=runstore
        )
        if run is not None and state.is_success():
            logging.info(f'run {run_id} is a success')
            workflow = run.workflow
            if workflow.run_postproc:
                # Get the latest ranking for the workflow and create a
                # sorted list of run identifier to compare agains the
                # current post-processing key for the workflow.
                ranking = self.ranking_manager.get_ranking(workflow=workflow)
                runs = sorted([r.run_id for r in ranking])
                # Run post-processing task synchronously if the current
                # post-processing resources where generated for a different
                # set of runs than those in the ranking.
                if runs != workflow.ranking():
                    # Create temporary post-processing folder for static
                    # workflow files.
                    # postproc_store = self.fs.get_store_for_folder(key=run_tmpdir())
                    logging.info(f'Run post-processing workflow for {workflow.workflow_id}')
                    run_postproc_workflow(
                        workflow=workflow,
                        ranking=ranking,
                        keys=runs,
                        run_manager=self.run_manager,
                        tmpstore=self.fs.get_store_for_folder(key=run_tmpdir()),
                        staticfs=self.fs.get_store_for_folder(
                            key=dirs.workflow_staticdir(workflow.workflow_id)
                        ),
                        backend=self.backend
                    )


# -- Helper functions ---------------------------------------------------------

def run_postproc_workflow(
    workflow: WorkflowObject, ranking: List[RunResult],
    keys: List[str], run_manager: RunManager, tmpstore: StorageVolume,
    staticfs: StorageVolume, backend: WorkflowController
):
    """Run post-processing workflow for a workflow template.

    Parameters
    ----------
    workflow: flowserv.model.base.WorkflowObject
        Handle for the workflow that triggered the post-processing workflow run.
    ranking: list(flowserv.model.ranking.RunResult)
        List of runs in the current result ranking.
    keys: list of string
        Sorted list of run identifier for runs in the ranking.
    run_manager: flowserv.model.run.RunManager
        Manager for workflow runs
    tmpstore: flowserv.volume.base.StorageVolume
        Temporary storage volume where the created post-processing files are
        stored. This volume will be erased after the workflow is started.
    staticfs: flowserv.volume.base.StorageVolume
        Storage volume that contains the static files from the workflow
        template.
    backend: flowserv.controller.base.WorkflowController
        Backend that is used to execute the post-processing workflow.
    """
    # Get workflow specification and the list of input files from the
    # post-processing statement.
    postproc_spec = workflow.postproc_spec
    workflow_spec = postproc_spec.get('workflow')
    pp_inputs = postproc_spec.get('inputs', {})
    pp_files = pp_inputs.get('files', [])
    # Prepare temporary directory with result files for all
    # runs in the ranking. The created directory is the only
    # run argument
    strace = None
    try:
        prepare_postproc_data(
            input_files=pp_files,
            ranking=ranking,
            run_manager=run_manager,
            store=tmpstore
        )
        dst = pp_inputs.get('runs', RUNS_DIR)
        run_args = {PARA_RUNS: InputDirectory(store=tmpstore, target=RUNS_DIR)}
        arg_list = [serialize_arg(PARA_RUNS, dst)]
    except Exception as ex:
        logging.error(ex, exc_info=True)
        strace = util.stacktrace(ex)
        run_args = dict()
        arg_list = []
    # Create a new run for the workflow. The identifier for the run group is
    # None.
    run = run_manager.create_run(
        workflow=workflow,
        arguments=arg_list,
        runs=keys
    )
    if strace is not None:
        # If there were data preparation errors set the created run into an
        # error state and return.
        run_manager.update_run(
            run_id=run.run_id,
            state=run.state().error(messages=strace)
        )
    else:
        # Execute the post-processing workflow asynchronously if
        # there were no data preparation errors.
        try:
            postproc_state, runstore = backend.exec_workflow(
                run=run,
                template=WorkflowTemplate(
                    workflow_spec=workflow_spec,
                    parameters=PARAMETERS
                ),
                arguments=run_args,
                staticfs=staticfs,
                config=workflow.engine_config
            )
        except Exception as ex:
            # Make sure to catch exceptions and set the run into an error state.
            postproc_state = run.state().error(messages=util.stacktrace(ex))
            runstore = None
        # Update the post-processing workflow run state if it is
        # no longer pending for execution.
        if not postproc_state.is_pending():
            run_manager.update_run(
                run_id=run.run_id,
                state=postproc_state,
                runstore=runstore
            )
        # Erase the temporary storage volume.
        tmpstore.erase()
