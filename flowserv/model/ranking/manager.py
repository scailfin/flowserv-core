# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module contains the ranking manager that is responsible for maintaining
and querying analytics results for individual workflows.
"""

from flowserv.model.ranking.base import ResultRanking, RunResult

import flowserv.core.error as err
import flowserv.model.parameter.declaration as pd
import flowserv.core.util as util


# -- Result table name for workflows ------------------------------------------

"""Each workflow defines an individual schema for the results of workflow
runs. The results are stored in a separate result table for each workflow. The
tables are created in the underlying database when the workflow is added to
the repository. The name of the result table is the identifier of the workflow
with a constant prefix. The prefix is necessary since workflow identifier may
start with a digit instaed of a letter which would lead to invalid table names.
"""


"""Prefix for columns in the result table to avoid conflicts with database
keywords.
"""
TEMPLATE_COL = 'col_{}'


def RESULT_TABLE(workflow_id):
    """Get default result table name for the workflow with the given
    identifier. It is assumed that the identifier is a UUID that only contains
    letters and digits and no white space or special characters.

    Parameters
    ----------
    workflow_id: string
        Unique workflow identifier

    Returns
    -------
    string
    """
    return 'res_{}'.format(workflow_id)


# -- Ranking Manager ----------------------------------------------------------

class RankingManager(object):
    """The ranking manager maintains leader boards for individual workflows.
    Analytics results for each workflow are maintaind in separate tables. The
    schema of thoses tables is defined by the result schema of the respective
    workflow template.
    """
    def __init__(self, con):
        """Initialize the connection to the underlying database.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        """
        self.con = con

    def get_ranking(
        self, workflow_id, result_schema, order_by=None, include_all=False
    ):
        """Query the underlying database to retrieve a result ranking for a
        given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        result_schema: flowserv.model.template.schema.ResultSchema
            Schema definition for workflow results
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given, the
            schema default sort order is used
        include_all: bool, optional
            Include at most one entry per group in the result if False

        Returns
        -------
        flowserv.model.ranking.ResultRanking

        Raises
        ------
        flowserv.core.error.InvalidSortColumnError
        """
        # Mapping of schema column names to renamed column identifier
        mapping = result_schema.rename()
        # Create SELECT clause containing a renaming of result table columns
        sql = 'SELECT g.group_id, g.name, '
        sql += 'r.run_id, r.created_at, r.started_at, r.ended_at'
        if not result_schema.is_empty():
            col_names = ['v.{} AS {}'.format(TEMPLATE_COL.format(c), mapping[c]) for c in mapping]
            sql += ', {} '.format(','.join(col_names))
            sql += 'FROM workflow_group g, workflow_run r, '
            sql += '{} v '.format(RESULT_TABLE(workflow_id))
            sql += 'WHERE g.group_id = r.group_id AND r.run_id = v.run_id'
            # Create ORDER BY clause
            sort_stmt = list()
            if order_by is None:
                order_by = result_schema.get_default_order()
            for col in order_by:
                # Ensure that the order by column exists in the result schema
                if col.identifier not in mapping:
                    raise err.InvalidSortColumnError(col.identifier)
                stmt_el = mapping[col.identifier]
                if col.sort_desc:
                    stmt_el += ' DESC'
                sort_stmt.append(stmt_el)
            sql += ' ORDER BY {}'.format(','.join(sort_stmt))
        else:
            sql += ' FROM workflow_group g, workflow_run r'
            sql += ' WHERE g.group_id = r.group_id'
            # Add optional filter to WHERE clause
        # Query the database to get the ordered list of workflow run results
        rs = self.con.execute(sql).fetchall()
        # Keep track of groups for which we have results (only needed if the
        # all_entries flag is False)
        groups = set()
        ranking = ResultRanking(columns=result_schema.columns)
        for row in rs:
            group_id = row['group_id']
            # Skip entry if this is not the first result for the group and
            # the include_all flag is false.
            if not include_all:
                if group_id in groups:
                    continue
                groups.add(group_id)
            # Add entry to leaderboard
            values = dict()
            for col_id in mapping:
                values[col_id] = row[mapping[col_id]]
            ranking.entries.append(
                RunResult(
                    run_id=row['run_id'],
                    group_id=group_id,
                    group_name=row['name'],
                    created_at=util.to_datetime(row['created_at']),
                    started_at=util.to_datetime(row['started_at']),
                    finished_at=util.to_datetime(row['ended_at']),
                    values=values
                )
            )
        return ranking

    def insert_result(
        self, workflow_id, result_schema, run_id, resources,
        commit_changes=True
    ):
        """Insert the results of a successful workflow run into the respective
        result table of the underlying database.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        result_schema: flowserv.model.template.schema.ResultSchema
            Result schema for the workflow
        run_id: string
            Unique run identifier
        resources: flowserv.model.workflow.resource.ResourceSet
            Set of resources that were generated by a successful
            workflow run
        commit_changes: bool, optional
            Commit all changes to the database if true

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        """
        # Read the results from the result file that is specified in the
        # workflow result schema. If the file is not found we currently
        # do not raise an error.
        f = resources.get_resource(name=result_schema.result_file)
        if f is None:
            return
        results = util.read_object(f.filename)
        # Create the DML statement to insert the results into the database
        columns = list(['run_id'])
        values = list([run_id])
        for col in result_schema.columns:
            val = util.jquery(doc=results, path=col.jpath())
            if val is None and col.required:
                msg = "missing value for '{}'"
                raise err.ConstraintViolationError(msg.format(col.identifier))
            columns.append(TEMPLATE_COL.format(col.identifier))
            values.append(val)
        # Execute insert statement (does not commit changes).
        sql = 'INSERT INTO {}({}) VALUES({})'.format(
             RESULT_TABLE(workflow_id),
             ','.join(columns),
             ','.join(['?'] * len(columns))
        )
        self.con.execute(sql, values)

    def register_workflow(self, workflow_id, result_schema, commit_changes=True):
        """Create the result table in the underlying database for a workflow
        with the given identifier and the given result schema.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        result_schema: flowserv.template.schema.ResultSchema
            Benchmark result schema specification
        commit_changes: bool, optional
            Commit all changes to the database if True
        """
        result_table = RESULT_TABLE(workflow_id)
        cols = list(['run_id CHAR(32) NOT NULL'])
        for col in result_schema.columns:
            stmt = TEMPLATE_COL.format(col.identifier)
            if col.data_type == pd.DT_INTEGER:
                stmt += ' INTEGER'
            elif col.data_type == pd.DT_DECIMAL:
                # Data type REAL is supported by PostgreSQL and SQLite3 but
                # potentially not by Orcale or other database management
                # systems. It may be necessary in the future to use a
                # DBMS-dendent value here (e.g., based on information that is
                # provided by the given database connection)
                stmt += ' REAL'
            else:
                stmt += ' TEXT'
            if col.required:
                stmt += ' NOT NULL'
            cols.append(stmt)
        sql = 'CREATE TABLE {}({}, PRIMARY KEY(run_id))'
        self.con.execute(sql.format(result_table, ', '.join(cols)))
        if commit_changes:
            self.con.commit()
