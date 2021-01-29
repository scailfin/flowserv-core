# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow resources."""

from typing import Dict, List, Optional

from flowserv.model.base import GroupObject, RunObject, WorkflowObject
from flowserv.model.ranking import RunResult
from flowserv.view.group import WorkflowGroupSerializer
from flowserv.view.run import RunSerializer


"""Serialization labels."""
COLUMN_NAME = 'name'
COLUMN_TITLE = 'label'
COLUMN_TYPE = 'dtype'
COLUMN_VALUE = 'value'
GROUP_ID = 'id'
GROUP_NAME = 'name'
PARAGROUP_INDEX = 'index'
PARAGROUP_NAME = 'name'
PARAGROUP_TITLE = 'title'
POSTPROC_RUN = 'postproc'
RANKING = 'ranking'
RUN_CREATED = 'createdAt'
RUN_FINISHED = 'finishedAt'
RUN_ID = 'id'
RUN_RESULTS = 'results'
RUN_STARTED = 'startedAt'
WORKFLOW_DESCRIPTION = 'description'
WORKFLOW_ID = 'id'
WORKFLOW_INSTRUCTIONS = 'instructions'
WORKFLOW_GROUP = 'group'
WORKFLOW_LIST = 'workflows'
WORKFLOW_PARAGROUPS = 'parameterGroups'
WORKFLOW_NAME = 'name'
WORKFLOW_PARAMETERS = 'parameters'
WORKFLOW_RUN = 'run'
WORKFLOW_SCHEMA = 'schema'


class WorkflowSerializer(object):
    """Default serializer for workflow resource objects. Defines the methods
    that are used to serialize workflow descriptors, handles, and listing.
    """
    def __init__(
        self, groups: Optional[WorkflowGroupSerializer] = None,
        runs: Optional[RunSerializer] = None
    ):
        """Initialize the serializer for run handles. The run serializer is
        required to serialize run handles that are part of a workflow handle
        with post-porcessing results.

        Parameters
        ----------
        groups: flowserv.view.group.WorkflowGroupSerializer, default=None
            Serializer for workflow groups.
        runs: flowserv.view.run.RunSerializer, default=None
            Serializer for run handles.
        """
        self.groups = groups if groups is not None else WorkflowGroupSerializer()
        self.runs = runs if runs is not None else RunSerializer()

    def workflow_descriptor(self, workflow: WorkflowObject) -> Dict:
        """Get dictionary serialization containing the descriptor of a
        workflow resource.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowObject
            Workflow descriptor.

        Returns
        -------
        dict
        """
        obj = {
            WORKFLOW_ID: workflow.workflow_id,
            WORKFLOW_NAME: workflow.name
        }
        if workflow.description is not None:
            obj[WORKFLOW_DESCRIPTION] = workflow.description
        if workflow.instructions is not None:
            obj[WORKFLOW_INSTRUCTIONS] = workflow.instructions
        return obj

    def workflow_handle(
        self, workflow: WorkflowObject, postproc: Optional[RunObject] = None,
        groups: Optional[List[GroupObject]] = None
    ) -> Dict:
        """Get dictionary serialization containing the handle of a workflow
        resource.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowObject
            Workflow handle
        postproc: flowserv.model.base.RunObject
            Handle for workflow post-porcessing run.
        groups: list(flowserv.model.base.GroupObject), default=None
            Optional list of descriptors for workflow groups for an
            authenticated user.

        Returns
        -------
        dict
        """
        obj = self.workflow_descriptor(workflow)
        # Add parameter declarations to the serialized workflow descriptor
        parameters = workflow.parameters.values() if workflow.parameters is not None else []
        obj[WORKFLOW_PARAMETERS] = [p.to_dict() for p in parameters]
        # Add parameter group definitions if defined for the workflow.
        parameter_groups = workflow.parameter_groups
        if parameter_groups is not None:
            obj[WORKFLOW_PARAGROUPS] = [
                {
                    PARAGROUP_NAME: g.name,
                    PARAGROUP_TITLE: g.title,
                    PARAGROUP_INDEX: g.index
                } for g in parameter_groups]
        # Add serialization for post-processing workflow (if present).
        if postproc is not None:
            obj[POSTPROC_RUN] = self.runs.run_handle(run=postproc)
        # Add users' workflow groups if given.
        if groups is not None:
            obj.update(self.groups.group_listing(groups=groups))
        return obj

    def workflow_leaderboard(
        self, workflow: WorkflowObject, ranking: List[RunResult],
        postproc: Optional[RunObject] = None
    ) -> Dict:
        """Get dictionary serialization for a workflow evaluation leaderboard.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowObject
            Workflow handle
        leaderboard: flowserv.model.ranking.ResultRanking
            List of entries in the workflow evaluation leaderboard
        postproc: flowserv.model.base.RunObject
            Handle for workflow post-porcessing run.

        Returns
        -------
        dict
        """
        # Serialize ranking entries
        entries = list()
        for run in ranking:
            results = list()
            for key in run.values:
                results.append({
                    COLUMN_NAME: key,
                    COLUMN_VALUE: run.values[key]
                })
            entries.append({
                WORKFLOW_RUN: {
                    RUN_ID: run.run_id,
                    RUN_CREATED: run.created_at,
                    RUN_STARTED: run.started_at,
                    RUN_FINISHED: run.finished_at
                },
                WORKFLOW_GROUP: {
                    GROUP_ID: run.group_id,
                    GROUP_NAME: run.group_name
                },
                RUN_RESULTS: results
            })
        # Add schema information for the leaderboard.
        schema = list()
        for c in workflow.result_schema.columns:
            schema.append({
                COLUMN_NAME: c.column_id,
                COLUMN_TITLE: c.name,
                COLUMN_TYPE: c.dtype
            })
        obj = {
            WORKFLOW_SCHEMA: schema,
            RANKING: entries
        }
        # Add serialization for optional workflow post-processing run handle
        if postproc is not None:
            obj[POSTPROC_RUN] = self.runs.run_handle(
                run=postproc
            )
        return obj

    def workflow_listing(self, workflows: List[WorkflowObject]) -> Dict:
        """Get dictionary serialization of a workflow listing.

        Parameters
        ----------
        workflows: list(flowserv.model.base.WorkflowObject)
            List of workflow descriptors

        Returns
        -------
        dict
        """
        return {WORKFLOW_LIST: [self.workflow_descriptor(w) for w in workflows]}
