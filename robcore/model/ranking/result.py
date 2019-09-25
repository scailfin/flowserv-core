# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Base classes that define the components of run result listings and leader
boards.
"""

import robcore.model.base as base
import robcore.util as util


class ResultRanking(object):
    """The result ranking contains a sorted list of run results. The schema of
    the respective result value dictionary os defined by the given column list.
    """
    def __init__(self, columns, entries=None):
        """Initialize the object properties

        Parameters
        ----------
        columns: list(robcore.model.template.schema.ResultColumn)
            List of columns in the result schema
        entrties: list(robapi.model.run.result.RunResult), optional
            List of entries in the result ranking
        """
        self.columns = columns
        self.entries = entries if not entries is None else list()

    @staticmethod
    def query(
        con, benchmark_id, schema, filter_stmt=None, args=None, order_by=None,
        include_all=False
    ):
        """Query the underlying database to retrieve a result ranking for a
        given benchmark.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        benchmark_id: string
            Unique benchmark identifier
        schema: robcore.model.template.schema.ResultSchema
            Schema definition for benchmark results
        filter_stmt: string, optional
            Optional partial filter statement for the WHERE clause.
        args: list or tuple
            List of argument values referenced in the filter statement
        order_by: list(robcore.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the schema
            default attribute is used
        include_all: bool, optional
            Include at most one entry per submission in the result if False

        Returns
        -------
        robapi.model.run.result.ResultRanking
        """
        # Mapping of schema column names to renamed column identifier
        mapping = schema.rename()
        # Create SELECT clause containing a renaming of result table columns
        sql = 'SELECT s.submission_id, s.name, '
        sql += 'r.run_id, r.created_at, r.started_at, r.ended_at'
        if not schema.is_empty():
            col_names = ['v.{} AS {}'.format(c, mapping[c]) for c in mapping]
            sql += ', {} '.format(','.join(col_names))
            sql += 'FROM benchmark_submission s, benchmark_run r, '
            sql += '{} v '.format(base.RESULT_TABLE(benchmark_id))
            sql += 'WHERE s.submission_id = r.submission_id AND r.run_id = v.run_id'
            # Add optional filter to WHERE clause
            if not filter_stmt is None:
                sql += ' AND ' + filter_stmt
            # Create ORDER BY clause
            if order_by is None:
                order_by = schema.get_default_order()
            sort_stmt = list()
            for col in order_by:
                stmt_el = mapping[col.identifier]
                if col.sort_desc:
                    stmt_el += ' DESC'
                sort_stmt.append(stmt_el)
            sql += ' ORDER BY {}'.format(','.join(sort_stmt))
        else:
            sql += ' FROM benchmark_submission s, benchmark_run r'
            sql += ' WHERE s.submission_id = r.submission_id'
            # Add optional filter to WHERE clause
            if not filter_stmt is None:
                sql += ' AND ' + filter_stmt
        # Query the database to get the ordered list or benchmark run results
        if not filter_stmt is None:
            rs = con.execute(sql, args).fetchall()
        else:
            rs = con.execute(sql).fetchall()
        # Keep track of users for which we have results (only needed if the
        # all_entries flag is False)
        submissions = set()
        ranking = ResultRanking(columns=schema.columns)
        for row in rs:
            submission_id = row['submission_id']
            # Skip entry of this is not the first result for the submission and
            # the include_all flag is false.
            if not include_all:
                if submission_id in submissions:
                    continue
                submissions.add(submission_id)
            # Add entry to leaderboard
            values = dict()
            for col_id in mapping:
                values[col_id] = row[mapping[col_id]]
            ranking.entries.append(
                RunResult(
                    run_id=row['run_id'],
                    submission_id=submission_id,
                    submission_name=row['name'],
                    created_at=util.to_datetime(row['created_at']),
                    started_at=util.to_datetime(row['started_at']),
                    finished_at=util.to_datetime(row['ended_at']),
                    values=values
                )
            )
        return ranking

    def get(self, index):
        """Get ranking element at the given position.

        Parameters
        ----------
        index: int
            Index position for ranking element

        Returns
        -------
        robapi.model.run.result.RunResult
        """
        return self.entries[index]

    def names(self):
        """List of unique identifier for columns in the result schema

        Returns
        -------
        list(string)
        """
        return [col.identifier for col in self.columns]

    def size(self):
        """Number of elements in the ranking.

        Returns
        -------
        int
        """
        return len(self.entries)


class RunResult(object):
    """Handle for the analytics results of a successful workflow run. Maintains
    the run identifier, run timestampls, submission names, and a dictionary
    containing the generated values. The elements in the dictionary are defined
    by the result schema in the respective benchmark template.
    """
    def __init__(
        self, run_id, submission_id, submission_name, created_at, started_at,
        finished_at, values
    ):
        """Initialize the object properties.

        Parameters
        ----------
        run_id: string
            Unique run identifier
        submission_id: string
            Unique submission identifier
        submission_name: string
            Human-readable name of the submission
        created_at: datetime.datetime
            Timestamp of workflow creation
        started_at: datetime.datetime
            Timestamp when the workflow started running
        finished_at: datetime.datetime, optional
            Timestamp when workflow execution completed
        values: dict
            Dictionary of benchmark analytics results
        """
        self.run_id = run_id
        self.submission_id = submission_id
        self.submission_name = submission_name
        self.created_at = created_at
        self.started_at = started_at
        self.finished_at = finished_at
        self.values = values

    def exectime(self):
        """The execution time for the workflow is the difference between the
        start timestamp and the end timestamp.

        Returns
        -------
        datetime.timedelta
        """
        return self.finished_at - self.started_at

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
