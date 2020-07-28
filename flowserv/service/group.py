# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow user group API component provides methods to access, create,
and manipulate workflow groups.
"""

import flowserv.error as err


class WorkflowGroupService(object):
    """API component that provides methods to access and manipulate workflow
    groups.
    """
    def __init__(
        self, group_manager, workflow_repo, backend, auth, serializer
    ):
        """Initialize the internal reference to the group manager, the workflow
        repository, and the serializer.

        Parameters
        ----------
        group_manager: flowserv.model.group..GroupManager
            Manager for workflow groups
        workflow_repo: flowserv.model.workflow.manager.WorkflowManager
            Repository for workflow templates
        backend: flowserv.controller.base.WorkflowController
            Workflow engine controller
        auth: flowserv.model.auth.Auth
            Implementation of the authorization policy for the API
        serializer: flowserv.view.group.WorkflowGroupSerialize
            Override the default serializer
        """
        self.group_manager = group_manager
        self.workflow_repo = workflow_repo
        self.backend = backend
        self.auth = auth
        self.serialize = serializer

    def create_group(self, workflow_id, name, user_id, members=None):
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
        user_id: string
            unique identifier for the user that is the group owner
        members: list(string), default=None
            List of user identifier for group members

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownWorkflowError
        """
        # Get the handle for for the given workflow. This will raise an
        # exception if the workflow is unknown.
        workflow = self.workflow_repo.get_workflow(workflow_id)
        # If additional parameters are given we need to modify the workflow
        # specification accordingly. This may raise an error if a given
        # parameter identifier is not unique.
        template = workflow.get_template()
        group = self.group_manager.create_group(
            workflow_id=workflow_id,
            name=name,
            user_id=user_id,
            parameters=template.parameters,
            workflow_spec=template.workflow_spec,
            members=members
        )
        return self.serialize.group_handle(group)

    def delete_group(self, group_id, user_id):
        """Delete a given workflow group and all associated runs and uploaded
        files. If the user is not a member of the group an unauthorized access
        error is raised.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        user_id: string
            Unique user identifier

        Raises
        ------
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to delete the
        # workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        self.group_manager.delete_group(group_id)

    def get_group(self, group_id):
        """Get handle for workflow group with the given identifier.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnknownWorkflowGroupError
        """
        group = self.group_manager.get_group(group_id)
        return self.serialize.group_handle(group)

    def list_groups(self, workflow_id=None, user_id=None):
        """Get a listing of all workflow groups. If the user handle is given
        the result contains only those groups that the user is a member of.
        If the workflow identifier is given the result contains groups for that
        workflow only.

        Parameters
        ----------
        workflow_id: string, optional
            Unique workflow identifier
        user_id: string, optional
            Unique user identifier

        Returns
        -------
        dict
        """
        groups = self.group_manager.list_groups(
            workflow_id=workflow_id,
            user_id=user_id
        )
        return self.serialize.group_listing(groups)

    def update_group(self, group_id, user_id, name=None, members=None):
        """Update the name for the workflow group with the given identifier.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        user_id: string
            Unique user identifier
        name: string, optional
            New workflow group name
        members: list(string), optional
            Modified list of team members

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnauthorizedAccessError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Raise an error if the user does not have rights to update the
        # workflow group or if the workflow group does not exist.
        if not self.auth.is_group_member(group_id=group_id, user_id=user_id):
            raise err.UnauthorizedAccessError()
        group = self.group_manager.update_group(
            group_id=group_id,
            name=name,
            members=members
        )
        return self.serialize.group_handle(group)
