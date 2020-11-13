# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow resources."""

from flowserv.view.base import Serializer


class WorkflowSerializer(Serializer):
    """Default serializer for workflow resource objects. Defines the methods
    that are used to serialize workflow descriptors, handles, and listing.
    """
    def __init__(self, runs, labels=None):
        """Initialize serialization labels and the serializer for run handles.
        The run serializer is required to serialize run handles that are part
        of a workflow handle with post-porcessing results.

        Parameters
        ----------
        runs: flowserv.view.run.RunSerializer
            Serializer for run handles
        labels: object, optional
            Object instance that contains the values for serialization labels
        """
        super(WorkflowSerializer, self).__init__(
            labels={
                'COLUMN_NAME': 'name',
                'COLUMN_TITLE': 'label',
                'COLUMN_TYPE': 'dtype',
                'COLUMN_VALUE': 'value',
                'GROUP_ID': 'id',
                'GROUP_NAME': 'name',
                'PARAGROUP_INDEX': 'index',
                'PARAGROUP_NAME': 'name',
                'PARAGROUP_TITLE': 'title',
                'POSTPROC_RUN': 'postproc',
                'RANKING': 'ranking',
                'RUN_CREATED': 'createdAt',
                'RUN_FINISHED': 'finishedAt',
                'RUN_ID': 'id',
                'RUN_RESULTS': 'results',
                'RUN_STARTED': 'startedAt',
                'WORKFLOW_DESCRIPTION': 'description',
                'WORKFLOW_ID': 'id',
                'WORKFLOW_INSTRUCTIONS': 'instructions',
                'WORKFLOW_GROUP': 'group',
                'WORKFLOW_LIST': 'workflows',
                'WORKFLOW_PARAGROUPS': 'parameterGroups',
                'WORKFLOW_NAME': 'name',
                'WORKFLOW_PARAMETERS': 'parameters',
                'WORKFLOW_RUN': 'run',
                'WORKFLOW_SCHEMA': 'schema'
            },
            override_labels=labels
        )
        self.runs = runs

    def workflow_descriptor(self, workflow):
        """Get dictionary serialization containing the descriptor of a
        workflow resource.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowHDescriptor
            Workflow descriptor

        Returns
        -------
        dict
        """
        LABELS = self.labels
        obj = {
            LABELS['WORKFLOW_ID']: workflow.workflow_id,
            LABELS['WORKFLOW_NAME']: workflow.name
        }
        if workflow.description is not None:
            obj[LABELS['WORKFLOW_DESCRIPTION']] = workflow.description
        if workflow.instructions is not None:
            obj[LABELS['WORKFLOW_INSTRUCTIONS']] = workflow.instructions
        return obj

    def workflow_handle(self, workflow, postproc=None):
        """Get dictionary serialization containing the handle of a workflow
        resource.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowHandle
            Workflow handle
        postproc: flowserv.model.base.RunHandle
            Handle for workflow post-porcessing run.

        Returns
        -------
        dict
        """
        LABELS = self.labels
        obj = self.workflow_descriptor(workflow)
        # Add parameter declarations to the serialized workflow descriptor
        parameters = workflow.parameters.values()
        obj[LABELS['WORKFLOW_PARAMETERS']] = [p.to_dict() for p in parameters]
        # Add parameter group definitions if defined for the workflow.
        parameter_groups = workflow.parameter_groups
        if parameter_groups is not None:
            obj[LABELS['WORKFLOW_PARAGROUPS']] = [
                {
                    LABELS['PARAGROUP_NAME']: g.name,
                    LABELS['PARAGROUP_TITLE']: g.title,
                    LABELS['PARAGROUP_INDEX']: g.index
                } for g in parameter_groups]
        # Add serialization for post-processing workflow (if present).
        if postproc is not None:
            obj[LABELS['POSTPROC_RUN']] = self.runs.run_handle(run=postproc)
        return obj

    def workflow_leaderboard(self, workflow, ranking, postproc=None):
        """Get dictionary serialization for a workflow evaluation leaderboard.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowHandle
            Workflow handle
        leaderboard: flowserv.model.ranking.ResultRanking
            List of entries in the workflow evaluation leaderboard
        postproc: flowserv.model.base.RunHandle
            Handle for workflow post-porcessing run.

        Returns
        -------
        dict
        """
        LABELS = self.labels
        # Serialize ranking entries
        entries = list()
        for run in ranking:
            results = list()
            for key in run.values:
                results.append({
                    LABELS['COLUMN_NAME']: key,
                    LABELS['COLUMN_VALUE']: run.values[key]
                })
            entries.append({
                LABELS['WORKFLOW_RUN']: {
                    LABELS['RUN_ID']: run.run_id,
                    LABELS['RUN_CREATED']: run.created_at,
                    LABELS['RUN_STARTED']: run.started_at,
                    LABELS['RUN_FINISHED']: run.finished_at
                },
                LABELS['WORKFLOW_GROUP']: {
                    LABELS['GROUP_ID']: run.group_id,
                    LABELS['GROUP_NAME']: run.group_name
                },
                LABELS['RUN_RESULTS']: results
            })
        obj = {
            LABELS['WORKFLOW_SCHEMA']: [{
                    LABELS['COLUMN_NAME']: c.column_id,
                    LABELS['COLUMN_TITLE']: c.name,
                    LABELS['COLUMN_TYPE']: c.dtype
                } for c in workflow.result_schema.columns
            ],
            LABELS['RANKING']: entries
        }
        # Add serialization for optional workflow post-processing run handle
        if postproc is not None:
            obj[self.labels['POSTPROC_RUN']] = self.runs.run_handle(
                run=postproc
            )
        return obj

    def workflow_listing(self, workflows):
        """Get dictionary serialization of a workflow listing.

        Parameters
        ----------
        workflows: list(flowserv.model.base.WorkflowHandle)
            List of workflow descriptors

        Returns
        -------
        dict
        """
        LABELS = self.labels
        return {
            LABELS['WORKFLOW_LIST']: [
                self.workflow_descriptor(w) for w in workflows
            ]
        }
