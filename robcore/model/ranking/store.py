# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Methods to create, delete, and query benchmark run results in the underlying
database.
"""

import json

from robcore.model.template.schema import ResultSchema

import robcore.error as err
import robcore.model.template.parameter.declaration as pd


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


# -- Benchmark result rankings -------------------------------------------------

def create_result_table(con, benchmark_id, schema):
    """Create the result table in the underlying database for a benchmark with
    the given identifier and the given result schema.

    This method does not automatically commit changes to the underlying database
    since it is expected to be called from within other methods that may need
    to commit (or roll back) additional changes.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    benchmark_id: string
        Unique benchmark identifier
    schema: robcore.template.schema.ResultSchema
        Benchmark result schema specification
    """
    result_table = RESULT_TABLE(benchmark_id)
    cols = list(['run_id  CHAR(32) NOT NULL'])
    for col in schema.columns:
        stmt = col.identifier
        if col.data_type == pd.DT_INTEGER:
            stmt += ' INTEGER'
        elif col.data_type == pd.DT_DECIMAL:
            stmt += ' DOUBLE'
        else:
            stmt += ' TEXT'
        if col.required:
            stmt += ' NOT NULL'
        cols.append(stmt)
    sql = 'CREATE TABLE {}({}, PRIMARY KEY(run_id))'
    con.execute(sql.format(result_table, ','.join(cols)))


def get_leaderboard(self, benchmark_id, order_by=None, include_all=False):
    """Get current leaderboard for a given benchmark. The result is a
    ranking of run results. Each entry contains the run and submission
    information, as well as a dictionary with the results of the respective
    workflow run.

    If the include_all flag is False at most one result per submission is
    included in the result.

    Parameters
    ----------
    benchmark_id: string
        Unique benchmark identifier
    order_by: list(robcore.model.template.schema.SortColumn), optional
        Use the given attribute to sort run results. If not given the schema
        default attribute is used
    include_all: bool, optional
        Include at most one entry per submission in the result if False

    Returns
    -------
    robapi.model.run.result.ResultRanking

    Raises
    ------
    robcore.error.UnknownBenchmarkError
    """
    # Get the result schema for the benchmark. Will raise an error if the
    # benchmark does not exist.
    sql = 'SELECT result_schema FROM benchmark WHERE benchmark_id = ?'
    row = self.con.execute(sql, (benchmark_id,)).fetchone()
    if row is None:
        raise err.UnknownBenchmarkError(benchmark_id)
    # Get the result schema as defined in the workflow template
    if not row['result_schema'] is None:
        schema = ResultSchema.from_dict(json.loads(row['result_schema']))
    else:
        schema = ResultSchema()
    return ResultRanking.query(
        con=self.con,
        benchmark_id=benchmark_id,
        schema=schema,
        filter_stmt='s.benchmark_id = ?',
        args=(benchmark_id,),
        order_by=order_by,
        include_all=include_all
    )


def insert_run_results(con, run_id, files):
    """Insert the results of a successful benchmark run into the respective
    result table of the underlying database. A benchmark may not have a result
    schema associated with it in which case no changes will be/can be made.


    This method does not automatically commit changes to the underlying database
    since it is expected to be called from within other methods that may need
    to commit (or roll back) additional changes.

    Parameters
    ----------
    con: DB-API 2.0 database connection
        Connection to underlying database
    run_id: string
        Unique run identifier
    files: dict(robcore.model.workflow.resource.FileResource)
        Dictionary of result files that were generated by a successful workflow
        run

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
        # benchmark result schema
        results = util.read_object(files[schema.result_file_id].filename)
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
