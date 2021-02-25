# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow user group API component provides methods to access, create,
and manipulate workflow groups.
"""

from typing import Dict, List, Optional

from flowserv.controller.base import WorkflowController
from flowserv.model.auth import Auth
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.parameter.base import Parameter
from flowserv.model.run import RunManager
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.service.group.base import WorkflowGroupService
from flowserv.view.group import WorkflowGroupSerializer

import flowserv.error as err


class LocalWorkflowGroupService(WorkflowGroupService):
    """API component that provides methods to access and manipulate workflow
    groups.
    """
    def __init__(
        self, group_manager: WorkflowGroupManager, workflow_repo: WorkflowManager,
        backend: WorkflowController, run_manager: RunManager, auth: Auth,
        user_id: Optional[str] = None, serializer: Optional[WorkflowGroupSerializer] = None
    ):
        """Initialize the internal reference to the group manager, the workflow
        repository, and the serializer.

        Parameters
        ----------
        group_manager: flowserv.model.group.WorkflowGroupManager
            Manager for workflow groups
        workflow_repo: flowserv.model.workflow.manager.WorkflowManager
            Repository for workflow templates
        backend: flowserv.controller.base.WorkflowController
            Workflow engine controller
        run_manager: flowserv.model.run.RunManager
            Manager for workflow runs. The run manager is used to fetch the
            list of runs for an authenticated user.
        auth: flowserv.model.auth.Auth
            Implementation of the authorization policy for the API.
        user_id: string, default=None
            Identifier of an authenticated user.
        serializer: flowserv.view.group.WorkflowGroupSerializer
            Override the default serializer
        """
        self.group_manager = group_manager
        self.workflow_repo = workflow_repo
        self.backend = backend
        self.run_manager = run_manager
        self.auth = auth
        self.user_id = user_id
        self.serialize = serializer if serializer is not None else WorkflowGroupSerializer()

    def create_group(
        self, workflow_id: str, name: str, members: Optional[List[str]] = None,
        parameters: Optional[List[Parameter]] = None,
        engine_config: Optional[Dict] = None, identifier: Optional[str] = None
    ) -> Dict:
        """Create a new user group for a given workflow. Each group has a
        a unique name for the workflow, a group owner, and a list of additional
        group members.

        Workflow groups also define variants of the original workflow template
        by allowing to specify a list of additional template parameters.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        name: string
            Unique team name
        members: list(string), default=None
            List of user identifier for group members
        parameters: list of flowserv.model.parameter.base.Parameter, default=None
            Optional list of parameter declarations that are used to modify the
            template parameters for submissions of the created group.
        engine_config: dict, default=None
            Optional configuration settings that will be used as the default
            when running a workflow.
        identifier: string, default=None
            Optional user-provided group identifier.

        Returns
        -------
        dict
        """
        # Raise an error if the user is not authenticated.
        if self.user_id is None:
            raise err.UnauthorizedAccessError()
        # Get the handle for for the given workflow. This will raise an
        # exception if the workflow is unknown.
        workflow = self.workflow_repo.get_workflow(workflow_id)
        engine_config = engine_config if engine_config else workflow.engine_config
        # If additional parameters are given we need to modify the workflow
        # specification accordingly. This may raise an error if a given
        # parameter identifier is not unique.
        template = workflow.get_template()
        group = self.group_manager.create_group(
            workflow_id=workflow_id,
            name=name,
            user_id=self.user_id,
            parameters=parameters if parameters is not None else template.parameters,
            workflow_spec=template.workflow_spec,
            members=members,
            engine_config=engine_config,
            identifier=identifier
        )
        return self.serialize.group_handle(group)

    def delete_group(self, group_id: str):
        """Delete a given workflow group and all associated runs and uploaded
        files. If the user is not a member of the group an unauthorized access
        error is raised.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        """
        # Raise an error if the user does not have rights to delete the
        # workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        self.group_manager.delete_group(group_id)

    def get_group(self, group_id: str) -> Dict:
        """Get handle for workflow group with the given identifier.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        dict
        """
        group = self.group_manager.get_group(group_id)
        # Fetch user runs if a valid user identifier was given.
        runs = None
        if self.user_id is not None:
            runs = self.run_manager.list_runs(group_id=group_id)
        return self.serialize.group_handle(group=group, runs=runs)

    def list_groups(self, workflow_id: Optional[str] = None) -> Dict:
        """Get a listing of all workflow groups. The result contains only those
        groups that the user is a member of. If the workflow identifier is given
        as an additional filter, then the result contains a user's groups for
        that workflow only.

        Parameters
        ----------
        workflow_id: string, optional
            Unique workflow identifier

        Returns
        -------
        dict
        """
        groups = self.group_manager.list_groups(
            workflow_id=workflow_id,
            user_id=self.user_id
        )
        return self.serialize.group_listing(groups)

    def update_group(
        self, group_id: str, name: Optional[str] = None,
        members: Optional[List[str]] = None
    ) -> Dict:
        """Update the name for the workflow group with the given identifier.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        name: string, optional
            New workflow group name
        members: list(string), optional
            Modified list of team members

        Returns
        -------
        dict
        """
        # Raise an error if the user does not have rights to update the
        # workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=self.user_id):
            raise err.UnauthorizedAccessError()
        group = self.group_manager.update_group(
            group_id=group_id,
            name=name,
            members=members
        )
        return self.serialize.group_handle(group)
