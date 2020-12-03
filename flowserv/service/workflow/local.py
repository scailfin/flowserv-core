# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow API component provides methods to create and access workflows
and workflow result rankings.
"""

from typing import Dict, List, Optional

from flowserv.model.ranking import RankingManager
from flowserv.model.run import RunManager
from flowserv.model.template.schema import SortColumn
from flowserv.model.workflow.manager import WorkflowManager
from flowserv.service.workflow.base import WorkflowService
from flowserv.view.workflow import WorkflowSerializer


class LocalWorkflowService(WorkflowService):
    """API component that provides methods to access workflows and workflow
    evaluation rankings (benchmark leader boards).
    """
    def __init__(
        self, workflow_repo: WorkflowManager, ranking_manager: RankingManager,
        run_manager: RunManager, serializer: Optional[WorkflowSerializer] = None
    ):
        """Initialize the internal reference to the workflow repository, the
        ranking manager, and the resource serializer.

        Parameters
        ----------
        workflow_repo: flowserv.model.workflow.manager.WorkflowManager
            Repository to access registered workflows
        ranking_manager: flowserv.model.ranking.RankingManager
            Manager for workflow evaluation rankings
        run_manager: flowserv.model.run.RunManager
            Manager for workflow runs. The run manager is used to access
            prost-processing runs.
        serializer: flowserv.view.workflow.WorkflowSerializer, default=None
            Override the default serializer
        """
        self.workflow_repo = workflow_repo
        self.ranking_manager = ranking_manager
        self.run_manager = run_manager
        self.serialize = serializer if serializer is not None else WorkflowSerializer()

    def get_ranking(
        self, workflow_id: str, order_by: Optional[List[SortColumn]] = None,
        include_all: Optional[bool] = False
    ) -> Dict:
        """Get serialization of the evaluation ranking for the given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given, the
            schema default sort order is used
        include_all: bool, optional
            Include all entries (True) or at most one entry (False) per user
            group in the returned ranking

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnknownWorkflowError
        """
        # Get the workflow handle to ensure that the workflow exists
        workflow = self.workflow_repo.get_workflow(workflow_id)
        # Only ifthe workflow has a defined result schema we get theranking of
        # run results. Otherwise, the ranking is empty.
        if workflow.result_schema is not None:
            ranking = self.ranking_manager.get_ranking(
                workflow=workflow,
                order_by=order_by,
                include_all=include_all
            )
        else:
            ranking = list()
        postproc = None
        if workflow.postproc_run_id is not None:
            postproc = self.run_manager.get_run(workflow.postproc_run_id)
        return self.serialize.workflow_leaderboard(
            workflow=workflow,
            ranking=ranking,
            postproc=postproc
        )

    def get_workflow(self, workflow_id: str) -> Dict:
        """Get serialization of the handle for the given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.error.UnknownWorkflowError
        """
        # Get the workflow handle. This will ensure that the workflow exists.
        workflow = self.workflow_repo.get_workflow(workflow_id)
        postproc = None
        if workflow.postproc_run_id is not None:
            postproc = self.run_manager.get_run(workflow.postproc_run_id)
        return self.serialize.workflow_handle(workflow, postproc=postproc)

    def list_workflows(self) -> Dict:
        """Get serialized listing of descriptors for all workflows in the
        repository.

        Returns
        -------
        dict
        """
        workflows = self.workflow_repo.list_workflows()
        return self.serialize.workflow_listing(workflows)
