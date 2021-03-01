# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base class for the workflow user group API component that provides methods
to access, create, and manipulate workflow groups.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional

from flowserv.model.parameter.base import Parameter


class WorkflowGroupService(metaclass=ABCMeta):  # pragma: no cover
    """API component that provides methods to access and manipulate workflow
    groups.
    """
    @abstractmethod
    def create_group(
        self, workflow_id: str, name: str, members: Optional[List[str]] = None,
        parameters: Optional[List[Parameter]] = None, identifier: Optional[str] = None
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
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def delete_group(self, group_id: str):
        """Delete a given workflow group and all associated runs and uploaded
        files. If the user is not a member of the group an unauthorized access
        error is raised.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        """
        raise NotImplementedError()  # pragma: no cover

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
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
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
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
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
        raise NotImplementedError()  # pragma: no cover
