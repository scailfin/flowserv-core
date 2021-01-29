# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module contains the ranking manager that is responsible for maintaining
and querying analytics results for individual workflows.
"""

from dateutil.parser import isoparse

from flowserv.model.base import GroupObject, RunObject

import flowserv.model.workflow.state as st


class RunResult(object):
    """Handle for analytics results of a successful workflow run. Maintains the
    run identifier, run timestamps, group information, and a dictionary
    containing the generated values. The elements in the dictionary are defined
    by the result schema in the respective workflow template.
    """
    def __init__(
        self, run_id, group_id, group_name, created_at, started_at,
        finished_at, values
    ):
        """Initialize the object properties.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        group_id: string
            Unique workflow group identifier
        group_name: string
            Human-readable name of the workflow group
        created_at: datetime.datetime
            Timestamp of workflow creation
        started_at: datetime.datetime
            Timestamp when the workflow started running
        finished_at: datetime.datetime, optional
            Timestamp when workflow execution completed
        values: dict
            Dictionary of analytics results
        """
        self.run_id = run_id
        self.group_id = group_id
        self.group_name = group_name
        self.created_at = created_at
        self.started_at = started_at
        self.finished_at = finished_at
        self.values = values

    def exectime(self):
        """The execution time for the workflow run is the difference between
        the start timestamp and the end timestamp.

        Returns
        -------
        datetime.timedelta
        """
        return isoparse(self.finished_at) - isoparse(self.started_at)

    def get(self, name):
        """Get the result value for the schema attribute with the given name.

        Parameters
        ----------
        name: string
            Name of attribute in the result schema.

        Returns
        -------
        string, int, or float
        """
        return self.values.get(name)


class RankingManager(object):
    """The ranking manager maintains leader boards for individual workflows.
    Analytics results for each workflow are maintaind in separate tables. The
    schema of thoses tables is defined by the result schema of the respective
    workflow template.
    """
    def __init__(self, session):
        """Initialize the connection to the underlying database.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        """
        self.session = session

    def get_ranking(self, workflow, order_by=None, include_all=False):
        """Query the underlying database to retrieve a result ranking for a
        given workflow.

        Parameters
        ----------
        workflow: flowserv.model.base.WorkflowObject
            Handle for workflow.
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given, the
            schema default sort order is used
        include_all: bool, optional
            Include at most one entry per group in the result if False

        Returns
        -------
        list(flowserv.model.ranking.RunResult)
        """
        # Get results for all successful runs of the workflow.
        rs = self.session.query(GroupObject, RunObject)\
            .filter(GroupObject.group_id == RunObject.group_id)\
            .filter(GroupObject.workflow_id == workflow.workflow_id)\
            .filter(RunObject.state_type == st.STATE_SUCCESS)\
            .filter(RunObject.result != None)\
            .all()  # noqa: E711
        entries = list()
        for group, run in rs:
            # Add entry to leaderboard
            entries.append(
                RunResult(
                    run_id=run.run_id,
                    group_id=group.group_id,
                    group_name=group.name,
                    created_at=run.created_at,
                    started_at=run.started_at,
                    finished_at=run.ended_at,
                    values=run.result
                )
            )
        # Sort the ranking based on the order by clause. If no order by clause
        # is given use the schema default sort order..
        result_schema = workflow.result_schema
        if order_by is None:
            order_by = result_schema.get_default_order()
        for sort_col in order_by[::-1]:
            sort_key = sort_col.column_id
            entries = sorted(
                entries,
                key=lambda e: e.get(sort_key),
                reverse=sort_col.sort_desc
            )
        # Remove multiple entries for the same group if requested by the user.
        if not include_all:
            pruned_entries = list()
            groups = set()
            for item in entries:
                if item.group_id not in groups:
                    groups.add(item.group_id)
                    pruned_entries.append(item)
            entries = pruned_entries
        return entries
