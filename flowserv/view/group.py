# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow user groups."""

from typing import Dict, List, Optional

from flowserv.model.base import GroupObject, RunObject
from flowserv.view.files import UploadFileSerializer
from flowserv.view.run import RunSerializer


"""Serialization labels."""
ENGINE_CONFIG = 'config'
GROUP_ID = 'id'
GROUP_LIST = 'groups'
GROUP_MEMBERS = 'members'
GROUP_NAME = 'name'
GROUP_PARAMETERS = 'parameters'
GROUP_UPLOADS = 'files'
USER_ID = 'id'
USER_NAME = 'username'
WORKFLOW_ID = 'workflow'


class WorkflowGroupSerializer(object):
    """Default serializer for workflow user groups."""
    def __init__(
        self, files: Optional[UploadFileSerializer] = None,
        runs: Optional[RunSerializer] = None
    ):
        """Initialize the serializer for uploaded files and workflow runs.

        Parameters
        ----------
        files: flowserv.view.files.UploadFileSerializer, default=None
            Serializer for handles of uploaded files
        runs: flowserv.view.run.RunSerializer, default=None
            Serializer for run handles
        """
        self.files = files if files is not None else UploadFileSerializer()
        self.runs = runs if runs is not None else RunSerializer()

    def group_descriptor(self, group: GroupObject) -> Dict:
        """Get serialization for a workflow group descriptor. The descriptor
        contains the group identifier, name, and the base list of HATEOAS
        references.

        Parameters
        ----------
        group: flowserv.model.base.GroupObject
            Workflow group handle

        Returns
        -------
        dict
        """
        return {
            GROUP_ID: group.group_id,
            GROUP_NAME: group.name,
            WORKFLOW_ID: group.workflow_id
        }

    def group_handle(self, group: GroupObject, runs: Optional[List[RunObject]] = None) -> Dict:
        """Get serialization for a workflow group handle.

        Parameters
        ----------
        group: flowserv.model.base.GroupObject
            Workflow group handle
        runs: list of flowserv.model.base.RunObject, default=None
            Optional list of run handles for an authenticated user.

        Returns
        -------
        dict
        """
        doc = self.group_descriptor(group)
        members = list()
        for u in group.members:
            members.append({
                USER_ID: u.user_id,
                USER_NAME: u.name
            })
        doc[GROUP_MEMBERS] = members
        parameters = group.parameters.values()
        # Include group specific list of workflow template parameters
        doc[GROUP_PARAMETERS] = [p.to_dict() for p in parameters]
        # Include handles for all uploaded files
        files = list()
        for file in group.uploads:
            f = self.files.file_handle(
                group_id=group.group_id,
                fh=file
            )
            files.append(f)
        doc[GROUP_UPLOADS] = files
        # Add optional engine configuration
        doc[ENGINE_CONFIG] = group.engine_config
        # Include run handles if given.
        if runs is not None:
            doc.update(self.runs.run_listing(runs=runs))
        return doc

    def group_listing(self, groups: List[GroupObject]) -> Dict:
        """Get serialization of a workflow group descriptor list.

        Parameters
        ----------
        groups: list(flowserv.model.base.GroupObject)
            List of descriptors for workflow groups

        Returns
        -------
        dict
        """
        return {GROUP_LIST: [self.group_descriptor(g) for g in groups]}
