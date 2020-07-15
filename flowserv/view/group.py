# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow user groups."""

from flowserv.view.base import Serializer


class WorkflowGroupSerializer(Serializer):
    """Default serializer for workflow user groups."""
    def __init__(self, files, labels=None):
        """Initialize serialization labels.

        Parameters
        ----------
        files: flowserv.view.files.UploadFileSerializer
            Serializer for handles of uploaded files
        labels: object, optional
            Object instance that contains the values for serialization labels
        """
        super(WorkflowGroupSerializer, self).__init__(
            labels={
                'GROUP_ID': 'id',
                'GROUP_LIST': 'groups',
                'GROUP_MEMBERS': 'members',
                'GROUP_NAME': 'name',
                'GROUP_PARAMETERS': 'parameters',
                'GROUP_UPLOADS': 'files',
                'USER_ID': 'id',
                'USER_NAME': 'username',
                'WORKFLOW_ID': 'workflow'
            },
            override_labels=labels
        )
        self.files = files

    def group_descriptor(self, group):
        """Get serialization for a workflow group descriptor. The descriptor
        contains the group identifier, name, and the base list of HATEOAS
        references.

        Parameters
        ----------
        group: flowserv.model.base.GroupHandle
            Workflow group handle

        Returns
        -------
        dict
        """
        LABELS = self.labels
        return {
            LABELS['GROUP_ID']: group.group_id,
            LABELS['GROUP_NAME']: group.name,
            LABELS['WORKFLOW_ID']: group.workflow_id
        }

    def group_handle(self, group):
        """Get serialization for a workflow group handle.

        Parameters
        ----------
        group: flowserv.model.base.GroupHandle
            Workflow group handle

        Returns
        -------
        dict
        """
        LABELS = self.labels
        doc = self.group_descriptor(group)
        members = list()
        for u in group.members:
            members.append({
                LABELS['USER_ID']: u.user_id,
                LABELS['USER_NAME']: u.name
            })
        doc[LABELS['GROUP_MEMBERS']] = members
        parameters = group.parameters.values()
        # Include group specific list of workflow template parameters
        doc[LABELS['GROUP_PARAMETERS']] = [p.to_dict() for p in parameters]
        # Include handles for all uploaded files
        files = list()
        for file in group.uploads:
            f = self.files.file_handle(
                group_id=group.group_id,
                fh=file
            )
            files.append(f)
        doc[LABELS['GROUP_UPLOADS']] = files

        return doc

    def group_listing(self, groups):
        """Get serialization of a workflow group descriptor list.

        Parameters
        ----------
        groups: list(flowserv.model.base.GroupHandle)
            List of descriptors for workflow groups

        Returns
        -------
        dict
        """
        LABELS = self.labels
        return {
            LABELS['GROUP_LIST']: [self.group_descriptor(g) for g in groups]
        }
