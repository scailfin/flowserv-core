# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for workflow resources."""

from flowserv.view.base import Serializer

import flowserv.model.template.base as tmpl


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
                'COLUMN_ID': 'id',
                'COLUMN_NAME': 'name',
                'COLUMN_TYPE': 'type',
                'COLUMN_VALUE': 'value',
                'GROUP_ID': 'id',
                'GROUP_NAME': 'name',
                'MODULE_ID': 'id',
                'MODULE_INDEX': 'index',
                'MODULE_NAME': 'name',
                'POSTPROC_OUTPUTS': 'outputs',
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
                'WORKFLOW_MODULES': 'modules',
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
        workflow: flowserv.model.workflow.base.WorkflowHDescriptor
            Workflow descriptor

        Returns
        -------
        dict
        """
        LABELS = self.labels
        obj = {
            LABELS['WORKFLOW_ID']: workflow.identifier,
            LABELS['WORKFLOW_NAME']: workflow.name
        }
        if workflow.has_description():
            obj[LABELS['WORKFLOW_DESCRIPTION']] = workflow.description
        if workflow.has_instructions():
            obj[LABELS['WORKFLOW_INSTRUCTIONS']] = workflow.instructions
        return obj

    def workflow_handle(self, workflow):
        """Get dictionary serialization containing the handle of a workflow
        resource.

        Parameters
        ----------
        workflow: flowserv.model.workflow.base.WorkflowHandle
            Workflow handle

        Returns
        -------
        dict
        """
        LABELS = self.labels
        obj = self.workflow_descriptor(workflow)
        template = workflow.get_template()
        # Add parameter declarations to the serialized workflow descriptor
        parameters = template.parameters.values()
        obj[LABELS['WORKFLOW_PARAMETERS']] = [p.to_dict() for p in parameters]
        # Add module definitions if given
        modules = template.modules
        if modules is not None:
            obj[LABELS['WORKFLOW_MODULES']] = [
                {
                    LABELS['MODULE_ID']: m.identifier,
                    LABELS['MODULE_NAME']: m.name,
                    LABELS['MODULE_INDEX']: m.index
                } for m in modules]
        # Add serialization for post-processing workflow (if present).
        if workflow.postproc_run is not None:
            postproc_run = self.runs.run_handle(run=workflow.postproc_run)
            obj[LABELS['POSTPROC_RUN']] = postproc_run
            # Add output descriptors (if given)
            if tmpl.PPLBL_OUTPUTS in template.postproc_spec:
                postproc_outputs = template.postproc_spec[tmpl.PPLBL_OUTPUTS]
                obj[LABELS['POSTPROC_OUTPUTS']] = postproc_outputs
        return obj

    def workflow_leaderboard(self, workflow, ranking):
        """Get dictionary serialization for a workflow evaluation leaderboard.

        Parameters
        ----------
        workflow: flowserv.model.workflow.base.WorkflowHandle
            Workflow handle
        leaderboard: flowserv.model.ranking.ResultRanking
            List of entries in the workflow evaluation leaderboard

        Returns
        -------
        dict
        """
        LABELS = self.labels
        # Serialize ranking entries
        entries = list()
        for run in ranking.entries:
            results = list()
            for key in run.values:
                results.append({
                    LABELS['COLUMN_ID']: key,
                    LABELS['COLUMN_VALUE']: run.values[key]
                })
            entries.append({
                LABELS['WORKFLOW_RUN']: {
                    LABELS['RUN_ID']: run.run_id,
                    LABELS['RUN_CREATED']: run.created_at.isoformat(),
                    LABELS['RUN_STARTED']: run.started_at.isoformat(),
                    LABELS['RUN_FINISHED']: run.finished_at.isoformat()
                },
                LABELS['WORKFLOW_GROUP']: {
                    LABELS['GROUP_ID']: run.group_id,
                    LABELS['GROUP_NAME']: run.group_name
                },
                LABELS['RUN_RESULTS']: results
            })
        obj = {
            LABELS['WORKFLOW_SCHEMA']: [{
                    LABELS['COLUMN_ID']: c.identifier,
                    LABELS['COLUMN_NAME']: c.name,
                    LABELS['COLUMN_TYPE']: c.data_type
                } for c in ranking.columns
            ],
            LABELS['RANKING']: entries
        }
        # Add serialization for optional workflow post-processing run handle
        if workflow.postproc_run is not None:
            postproc_run = self.runs.run_handle(run=workflow.postproc_run)
            obj[self.labels['POSTPROC_RUN']] = postproc_run
            # Add output descriptors (if given)
            template = workflow.get_template()
            if tmpl.PPLBL_OUTPUTS in template.postproc_spec:
                postproc_outputs = template.postproc_spec[tmpl.PPLBL_OUTPUTS]
                obj[LABELS['POSTPROC_OUTPUTS']] = postproc_outputs
        return obj

    def workflow_listing(self, workflows):
        """Get dictionary serialization of a workflow listing.

        Parameters
        ----------
        workflows: list(flowserv.model.workflow.base.WorkflowDescriptor)
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
