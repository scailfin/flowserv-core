# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class for the workflow user group API component that provides methods
to access, create, and manipulate workflow groups.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional


class WorkflowGroupService(metaclass=ABCMeta):  # pragma: no cover
    """API component that provides methods to access and manipulate workflow
    groups.
    """
    @abstractmethod
    def create_group(
        self, workflow_id: str, name: str, user_id: str,
        members: Optional[List[str]] = None
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
        user_id: string
            unique identifier for the user that is the group owner
        members: list(string), default=None
            List of user identifier for group members

        Returns
        -------
        dict
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_group(self, group_id: str, user_id: str):
        """Delete a given workflow group and all associated runs and uploaded
        files. If the user is not a member of the group an unauthorized access
        error is raised.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        user_id: string
            Unique user identifier
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def list_groups(
        self, workflow_id: Optional[str] = None, user_id: Optional[str] = None
    ) -> Dict:
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
        raise NotImplementedError()

    @abstractmethod
    def update_group(
        self, group_id: str, user_id: str, name: Optional[str] = None,
        members: Optional[List[str]] = None
    ) -> Dict:
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
        """
        raise NotImplementedError()
