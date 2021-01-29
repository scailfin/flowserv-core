# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the group API component that provides access to workflow
user groups via a RESTful API.
"""

from typing import Dict, List, Optional

from flowserv.model.parameter.base import Parameter
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.group.base import WorkflowGroupService
from flowserv.service.remote import delete, get, post

import flowserv.service.descriptor as route
import flowserv.view.group as default_labels


class RemoteWorkflowGroupService(WorkflowGroupService):
    """API component that provides methods to access and manipulate workflow
    groups via a RESTful API.
    """
    def __init__(self, descriptor: ServiceDescriptor, labels: Optional[Dict] = None):
        """Initialize the Url route patterns from the service descriptor and
        the dictionary of labels for elements in request bodies.

        Parameters
        ----------
        descriptor: flowserv.service.descriptor.ServiceDescriptor
            Service descriptor containing the API route patterns.
        labels: dict, default=None
            Override the default labels for elements in request bodies.
        """
        # Default labels for elements in request bodies.
        self.labels = {
            'GROUP_ID': default_labels.GROUP_ID,
            'GROUP_NAME': default_labels.GROUP_NAME,
            'GROUP_MEMBERS': default_labels.GROUP_MEMBERS,
            'GROUP_PARAMETERS': default_labels.GROUP_PARAMETERS
        }
        if labels is not None:
            self.labels.update(labels)
        # Short cut to access urls from the descriptor.
        self.urls = descriptor.urls

    def create_group(
        self, workflow_id: str, name: str, members: Optional[List[str]] = None,
        parameters: Optional[List[Parameter]] = None, identifier: Optional[str] = None
    ) -> Dict:
        """Create a new user group for a given workflow. Each group has a
        a unique name for the workflow, and an optional list of additional
        group members. The group owener will be the authenticated user based
        on the access token that is provided in the request header.

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
        identifier: string, default=None
            Optional user-provided group identifier.

        Returns
        -------
        dict
        """
        # Create request body. The list of members and parameters are optional.
        data = {self.labels['GROUP_NAME']: name}
        if members is not None:
            data[self.labels['GROUP_MEMBERS']] = members
        if parameters is not None:
            data[self.labels['GROUP_PARAMETERS']] = [p.to_dict() for p in parameters]
        if identifier is not None:
            data[self.labels['GROUP_ID']] = identifier
        return post(url=self.urls(route.GROUPS_CREATE, workflowId=workflow_id), data=data)

    def delete_group(self, group_id: str):
        """Delete a given workflow group and all associated runs and uploaded
        files. If the user is not a member of the group an unauthorized access
        error is raised.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        """
        return delete(url=self.urls(route.GROUPS_DELETE, userGroupId=group_id))

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
        return get(url=self.urls(route.GROUPS_GET, userGroupId=group_id))

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
        # The request Url depends on whether a workflow identifier is given
        # or not.
        if workflow_id is not None:
            return get(url=self.urls(route.WORKFLOWS_GROUPS, workflowId=workflow_id))
        else:
            return get(url=self.urls(route.GROUPS_LIST))

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
        # Create request body. The group name and the list of members are
        # both optional.
        data = dict()
        if name is not None:
            data[self.labels['GROUP_NAME']] = name
        if members is not None:
            data[self.labels['GROUP_MEMBERS']] = members
        return post(url=self.urls(route.GROUPS_UPDATE, userGroupId=group_id), data=data)
