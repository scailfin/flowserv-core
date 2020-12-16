# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation for the API service component that provides access to workflow
handles and ranking results. This implementation is for the service that accesses
a remote RESTful API.
"""

from typing import Dict, List, Optional

from flowserv.model.template.schema import SortColumn
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.remote import get
from flowserv.service.workflow.base import WorkflowService

import flowserv.service.descriptor as route


class RemoteWorkflowService(WorkflowService):
    """API component that provides methods to access workflows and workflow
    evaluation rankings (benchmark leader boards) via a RESTful API.
    """
    def __init__(self, descriptor: ServiceDescriptor):
        """Initialize the Url route patterns from the service descriptor.

        Parameters
        ----------
        descriptor: flowserv.service.descriptor.ServiceDescriptor
            Service descriptor containing the API route patterns.
        """
        # Short cut to access urls from the descriptor.
        self.urls = descriptor.urls

    def get_ranking(
        self, workflow_id: str, order_by: Optional[List[SortColumn]] = None,
        include_all: Optional[bool] = False
    ) -> Dict:
        """Get serialization of the evaluation ranking for the given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        order_by: list(flowserv.model.template.schema.SortColumn), default=None
            Use the given attribute to sort run results. If not given, the
            schema default sort order is used
        include_all: bool, default=False
            Include all entries (True) or at most one entry (False) per user
            group in the returned ranking

        Returns
        -------
        dict
        """
        # Create query string for the order by clause.
        q_order_by = list()
        if order_by is not None:
            for col in order_by:
                c = '{}:{}'.format(col.column_id, 'DESC' if col.sort_desc else 'ASC')
                q_order_by.append(c)
        url = self.urls(
            route.LEADERBOARD_GET,
            workflowId=workflow_id,
            orderBy=','.join(q_order_by),
            includeAll=include_all
        )
        return get(url=url)

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
        return get(url=self.urls(route.WORKFLOWS_GET, workflowId=workflow_id))

    def list_workflows(self) -> Dict:
        """Get serialized listing of descriptors for all workflows in the
        repository.

        Returns
        -------
        dict
        """
        return get(url=self.urls(route.WORKFLOWS_LIST))
