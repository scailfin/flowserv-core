# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow user groups."""

import flowserv.view.files as fileserializer
import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels


class WorkflowGroupSerializer(object):
    """Default serializer for workflow user groups."""
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: flowserv.view.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def group_descriptor(self, group):
        """Get serialization for a workflow group descriptor. The descriptor
        contains the group identifier, name, and the base list of HATEOAS
        references.

        Parameters
        ----------
        group: flowserv.model.group.base.WorkflowGroupDescriptor
            Workflow group handle

        Returns
        -------
        dict
        """
        g_id = group.identifier
        w_id = group.workflow_id
        return {
            labels.ID: g_id,
            labels.NAME: group.name,
            labels.WORKFLOW: w_id,
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.get_group(g_id),
                hateoas.WORKFLOW: self.urls.get_workflow(w_id),
                hateoas.action(hateoas.UPLOAD): self.urls.upload_file(g_id),
                hateoas.action(hateoas.SUBMIT): self.urls.start_run(g_id)

            })
        }

    def group_handle(self, group):
        """Get serialization for a workflow group handle.

        Parameters
        ----------
        group: flowserv.model.group.base.WorkflowGroupHandle
            Workflow group handle

        Returns
        -------
        dict
        """
        doc = self.group_descriptor(group)
        members = list()
        for u in group.members:
            members.append({labels.ID: u.identifier, labels.USERNAME: u.name})
        doc[labels.MEMBERS] = members
        parameters = group.parameters.values()
        # Include group specific list of workflow template parameters
        doc[labels.PARAMETERS] = [p.to_dict() for p in parameters]
        # Include handles for all uploaded files
        files = list()
        for file in group.list_files():
            f = fileserializer.file_handle(group_id=group.identifier, fh=file)
            files.append(f)
        doc[labels.FILES] = files

        return doc

    def group_listing(self, groups):
        """Get serialization of a workflow group descriptor list.

        Parameters
        ----------
        groups: list(flowserv.model.group.base.WorkflowGroupDescriptor)
            List of descriptors for workflow groups

        Returns
        -------
        dict
        """
        return {
            labels.GROUPS: [
                self.group_descriptor(g) for g in groups
            ],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_groups()
            })
        }
