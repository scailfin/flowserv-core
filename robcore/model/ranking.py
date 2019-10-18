# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module conatsins base classes that define the components of run result
listings and leader boards. The module also contains methods to create, delete,
and query benchmark run results in the underlying database.
"""

import json

from robcore.model.template.schema import ResultSchema

import robcore.error as err
import robcore.model.template.parameter.declaration as pd
import robcore.util as util


# -- Result ranking objects ----------------------------------------------------

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
        entrties: list(robcore.model.ranking.RunResult), optional
            List of entries in the result ranking
        """
        self.columns = columns
        self.entries = entries if not entries is None else list()

    def get(self, index):
        """Get ranking element at the given position.

        Parameters
        ----------
        index: int
            Index position for ranking element

        Returns
        -------
        robcore.model.ranking.RunResult
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
    the run identifier, run timestamps, submission information, and a dictionary
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


# -- Result table name for benchmarks ------------------------------------------

"""Each benchmark defines an individual schema for the results of benchmark
runs. The results are stored in separate result tables for each benchmark. The
tables are created in the underlying database when the benchmark is added to
the repository. The name of the result table is the identifier of the benchmark
with a constant prefix. The prefix is necessary since benchmark identifier may
start with a digit instaed of a letter which would lead to invalid table names.
"""
def RESULT_TABLE(identifier):
    """Get default result table name for the benchmark with the given
    identifier. It is assumed that the identifier is a UUID that only contains
    letters and digits and no white space or special characters.

    Parameters
    ----------
    identifier: string
        Unique benchmark identifier

    Returns
    -------
    string
    """
    return 'res_{}'.format(identifier)


# -- MAthods for maintaining and querying result rankings ----------------------

def create_result_table(con, benchmark_id, schema, commit_changes=True):
    """Create the result table in the underlying database for a benchmark with
    the given identifier and the given result schema.

    This method does not automatically commit changes to the underlying database
    since it is expected to be called from within other methods that may need
    to commit (or roll back) additional changes. Changes are only commited if
    the commit_changes flag is set to True.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    benchmark_id: string
        Unique benchmark identifier
    schema: robcore.template.schema.ResultSchema
        Benchmark result schema specification
    commit_changes: bool, optional
        Commit all changes to the database if true
    """
    result_table = RESULT_TABLE(benchmark_id)
    cols = list(['run_id CHAR(32) NOT NULL'])
    for col in schema.columns:
        stmt = col.identifier
        if col.data_type == pd.DT_INTEGER:
            stmt += ' INTEGER'
        elif col.data_type == pd.DT_DECIMAL:
            # Data type REAL is supported by PostgreSQL and SQLite3 but
            # potentially not by Orcale or other database management systems.
            # It may be necessary in the future to use a DBMS-dendent value
            # here (e.g., based on information that is provided by the given
            # database connection)
            stmt += ' REAL'
        else:
            stmt += ' TEXT'
        if col.required:
            stmt += ' NOT NULL'
        cols.append(stmt)
    sql = 'CREATE TABLE {}({}, PRIMARY KEY(run_id))'
    con.execute(sql.format(result_table, ', '.join(cols)))
    if commit_changes:
        con.commit()

def insert_run_results(con, run_id, files, commit_changes=True):
    """Insert the results of a successful benchmark run into the respective
    result table of the underlying database. A benchmark may not have a result
    schema associated with it in which case no changes will be/can be made.


    This method does not automatically commit changes to the underlying database
    since it is expected to be called from within other methods that may need
    to commit (or roll back) additional changes. Changes are only commited if
    the commit_changes flag is set to True.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    run_id: string
        Unique run identifier
    files: dict(robcore.model.workflow.resource.FileResource)
        Dictionary of result files that were generated by a successful workflow
        run
    commit_changes: bool, optional
        Commit all changes to the database if true

    Raises
    ------
    robcore.error.ConstraintViolationError
    """
    # Query the database in order to get the benchmark identifier and the
    # result schema. At this point we assume that the run exists (i.e., the
    # result is not empty). There may, however, not be a schema defined for
    # the benchmark. In this case the result table will not exist and not data
    # has to be written to the database.
    sql = 'SELECT b.benchmark_id, b.result_schema FROM '
    sql += 'benchmark b, benchmark_submission s, benchmark_run r '
    sql += 'WHERE r.run_id = ? AND r.submission_id = s.submission_id '
    sql += 'AND s.benchmark_id = b.benchmark_id'
    row = con.execute(sql, (run_id,)).fetchone()
    if not row['result_schema'] is None:
        # Create instance of the result schema from database serialization
        schema = ResultSchema.from_dict(json.loads(row['result_schema']))
        # Read the results from the result file that is specified in the
        # benchmark result schema. If the file is not found we currently
        # do not raise an error.
        f = files.get(schema.result_file_id)
        if f is None:
            return
        results = util.read_object(f.filename)
        # Create the DML statement to insert the results into the database
        columns = list(['run_id'])
        values = list([run_id])
        for col in schema.columns:
            if col.identifier in results:
                values.append(results[col.identifier])
            elif col.required:
                msg = 'missing value for \'{}\''
                raise err.ConstraintViolationError(msg.format(col.identifier))
            else:
                values.append(None)
            columns.append(col.identifier)
        # Execute insert statement (does not commit changes).
        sql = 'INSERT INTO {}({}) VALUES({})'.format(
             RESULT_TABLE(row['benchmark_id']),
            ','.join(columns),
            ','.join(['?'] * len(columns))
        )
        con.execute(sql, values)


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
    robcore.model.ranking.ResultRanking
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
        sql += '{} v '.format(RESULT_TABLE(benchmark_id))
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
