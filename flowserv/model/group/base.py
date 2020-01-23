# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow groups are containers for sets of users and sets of workflow runs.
Groups are primarily intended for benchmarks. For benchmarks, each group should
be viewed as an entry (or submission) to the benchmark.
"""


class WorkflowGroupDescriptor(object):
    """The descriptor for a workflow group contains only the group identifier,
    the group name, and the identifier of the associated workflow.
    """
    def __init__(self, identifier, name, workflow_id):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique group identifier
        name: string
            Unique group name
        workflow_id: string
            Unique workflow identifier
        """
        self.identifier = identifier
        self.name = name
        self.workflow_id = workflow_id


class WorkflowGroupHandle(WorkflowGroupDescriptor):
    """A workflow group is a container for sets of users and sets of workflow
    runs. Each group is associated with a workflow template.  When the group is
    created, variations to the original workflow may be made to the workflow
    specification and the template parameter declarations. The group handle
    maintains a modified copy of the respective parts of the workflow template.

    Each group has a name that uniquely identifies it among all groups for a
    workflow template. The group is created by a user (the owner) who can
    invite other users as group members.
    """
    def __init__(
        self, identifier, name, workflow_id, owner_id, parameters,
        workflow_spec, members=None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique group identifier
        name: string
            Unique group name
        workflow_id: string
            Unique workflow identifier
        owner_id: string
            Unique identifier for the user that created the group
        parameters: dict(string:flowserv.model.parameter.base.TemplateParameter)
            Workflow template parameter declarations
        workflow_spec: dict
            Workflow specification
        members: list(flowserv.model.user.base.UserHandle)
            List of handles for group members (includes the handle for the
            group owner if the owner is still a member)
        """
        super(WorkflowGroupHandle, self).__init__(
            identifier=identifier,
            name=name,
            workflow_id=workflow_id
        )
        self.owner_id = owner_id
        self.parameters = parameters
        self.workflow_spec = workflow_spec
        self.members = members
