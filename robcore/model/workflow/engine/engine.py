# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The benchmark engine is responsible initiating workflow runs and for
maintaining the state of running workflows.
"""

import json
import os

from robcore.io.files import InputFile
from robapi.model.run.base import RunHandle
from robcore.model.template.schema import ResultSchema
from robcore.model.workflow.state.base import StatePending

import robcore.error as err
import robcore.util as util


class BenchmarkEngine(object):
    """The benchmark engine executes benchmark workflows for a given set of
    argument values. The state of workflow runs in maintained in the underlying
    relational database.

    The engine provides methods to query and update the state of workflow runs.
    """
    def __init__(self, con, backend):
        """Initialize the connection to the databases that contains the
        benchmark result tables and the optional workflow execution backend.

        The given backend is used to execute benchmark runs.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to the underlying database
        backend: benchtmpl.backend.base.WorkflowEngine
            Workflow engine that is used to run benchmarks
        """
        self.con = con
        self.backend = backend

    def cancel_run(self, run_id):
        """Cancel the given run. This will raise an error if the run is not in
        an active state.

        Parameters
        ----------
        run_id: string
            Unique submission identifier

        Raises
        ------
        robcore.error.UnknownRunError
        robcore.error.InvalidRunStateError
        """
        # Get the run handle. This will raise an error if the run is unknown
        run = self.get_run(run_id)
        # Raise an error if the run is not in an active state
        if not run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Cancel execution at the backend
        self.backend.cancel_run(run_id)
        # Update the run state and return the run handle
        return self.update_run(run_id=run_id, state=run.state.cancel())

    def delete_run(self, run_id):
        """Delete the entry for the given run from the underlying database. This
        will also remove any run results and result file resources.

        Deleting a run will raise an error if the run is in an active state.

        Parameters
        ----------
        run_id: string
            Unique submission identifier

        Returns
        -------
        robapi.model.run.base.RunHandle

        Raises
        ------
        robcore.error.UnknownRunError
        robcore.error.InvalidRunStateError
        """
        # Get the handle for the runs to get the list of file resources that
        # have been created (if the run was executed successfully). This will
        # raise an error if the run is unknown.
        run = self.get_run(run_id)
        # Raise an exception if the run is active
        if run.is_active():
            raise err.InvalidRunStateError(run.state)
        # Start by deleting all rows in the database that belong to the run.
        # Delete files after the database changes are committed since there is
        # no rollback option for file deletes.
        psql = 'DELETE FROM {} WHERE run_id = ?'
        stmts = list()
        stmts.append(psql.format('run_result_file'))
        stmts.append(psql.format('run_error_log'))
        stmts.append(psql.format('benchmark_run'))
        for sql in stmts:
            self.con.execute(sql, (run_id,))
        self.con.commit()
        # Delete all file resources
        if run.is_success():
            for fh in run.get_files():
                # Don't raise an error if the file does not exist or cannot be
                # removed
                try:
                    os.remove(fh.filename)
                except OSError:
                    pass
        return run

    def get_run(self, run_id):
        """Get handle for the given run. The run state and associated submission
        and benchmark are read from the underlying database. This method does
        not query the backend to get the workflow state.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        robapi.model.run.base.RunHandle

        Raises
        ------
        robcore.error.UnknownRunError
        """
        # Fetch run information from the database. If the result is None the
        # run is unknown and an error is raised.
        sql = 'SELECT r.run_id, s.benchmark_id, s.submission_id, r.state, '
        sql += 'r.arguments, r.created_at, r.started_at, r.ended_at '
        sql += 'FROM benchmark_submission s, benchmark_run r '
        sql += 'WHERE s.submission_id = r.submission_id AND r.run_id = ?'
        row = self.con.execute(sql, (run_id,)).fetchone()
        if row is None:
            raise err.UnknownRunError(run_id)
        return RunHandle.from_db(doc=row, con=self.con)

    def start_run(self, submission_id, template, source_dir, arguments):
        """Run benchmark for given set of arguments.

        Parameters
        ----------
        run_id: string
            Unique identifier for the run
        template: robcore.model.template.base.WorkflowHandle
            Handle for benchmark that is being executed
        source_dir: string
            Source directory that contains the static template files
        arguments: dict(benchtmpl.workflow.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        robapi.model.run.base.RunHandle

        Raises
        ------
        benchtmpl.error.MissingArgumentError
        """
        # Create a unique run identifier
        run_id = util.get_unique_identifier()
        # Create an initial entry in the run table for the pending run.
        state = StatePending()
        sql = 'INSERT INTO benchmark_run('
        sql += 'run_id, submission_id, state, created_at, arguments'
        sql += ') VALUES(?, ?, ?, ?, ?)'
        ts = state.created_at.isoformat()
        arg_values = dict()
        for key in arguments:
            arg_values[key] = arguments[key].value
        arg_serilaization = json.dumps(arg_values, cls=ArgumentEncoder)
        values = (run_id, submission_id, state.type_id, ts, arg_serilaization)
        self.con.execute(sql, values)
        self.con.commit()
        # Execute the benchmark workflow for the given set of arguments.
        state = self.backend.execute(
            run_id=run_id,
            template=template,
            source_dir=source_dir,
            arguments=arguments
        )
        # Update the run state if it is no longer pending for execution.
        if not state.is_pending():
            self.update_run(run_id=run_id, state=state)
        return RunHandle(
            identifier=run_id,
            submission_id=submission_id,
            benchmark_id=template.identifier,
            state=state,
            arguments=json.loads(arg_serilaization)
        )

    def update_run(self, run_id, state):
        """Update the state of the given run. This method does check if the
        state transition is valid. Transitions are valid for active workflows,
        if the transition is (a) from pending to running or (b) to an inactive
        state. Invalid state transitions will raise an error.

        Parameters
        ----------
        run_id: string
            Unique identifier for the run
        state: benchtmpl.workflow.state.WorkflowState
            New workflow state

        Returns
        -------
        robapi.model.run.base.RunHandle

        Raises
        ------
        robcore.error.InvalidRunStateError
        robcore.error.UnknownRunError
        """
        # Retrieve the current state information to (a) ensure that the run
        # exists, and (b) validate that the state transition. This will raise
        # an error if the run does not exist.
        run = self.get_run(run_id)
        if not run.is_active():
            raise err.InvalidRunStateError(run.state, state)
        if run.is_running() and state.is_active():
            raise err.InvalidRunStateError(run.state, state)
        # Query template to update the state.
        sqltmpl = 'UPDATE benchmark_run SET state=\'' + state.type_id + '\''
        sqltmpl = sqltmpl + ', {} WHERE run_id = \'' + run_id + '\''
        stmts = list()
        # Only update the state in the database if the workflow is not pending.
        # For pending workflows an entry is created when the run starts.
        if state.is_running():
            stmts.append(
                (sqltmpl.format('started_at=?'),
                (state.started_at.isoformat(),))
            )
        elif state.is_canceled():
            stmts.append(
                (sqltmpl.format('started_at=?, ended_at=?'),
                (state.started_at.isoformat(), state.stopped_at.isoformat()))
            )
            # Insert statements for error messages
            instmpl = 'INSERT INTO run_error_log(run_id, message, pos) '
            instmpl += 'VALUES(?, ?, ?)'
            messages = state.messages
            for i in range(len(messages)):
                stmts.append((instmpl, (run_id, messages[i], i)))
        elif state.is_error():
            stmts.append(
                (sqltmpl.format('started_at=?, ended_at=?'),
                (state.started_at.isoformat(), state.stopped_at.isoformat()))
            )
            # Insert statements for error messages
            instmpl = 'INSERT INTO run_error_log(run_id, message, pos) '
            instmpl += 'VALUES(?, ?, ?)'
            messages = state.messages
            for i in range(len(messages)):
                stmts.append((instmpl, (run_id, messages[i], i)))
        elif state.is_success():
            stmts.append(
                (sqltmpl.format('started_at=?, ended_at=?'),
                (state.started_at.isoformat(), state.finished_at.isoformat()))
            )
            instmpl = 'INSERT INTO run_result_file(run_id, file_id, file_path) '
            instmpl += 'VALUES(?, ?, ?)'
            for f in state.files.values():
                stmts.append((instmpl, (run_id, f.identifier, f.filename)))
            # Create the DML statement to insert the result values. This
            # requires to query the database in order to get the result schema
            query = 'SELECT b.result_schema FROM '
            query += 'benchmark b, benchmark_submission s, benchmark_run r '
            query += 'WHERE r.run_id = ? AND r.submission_id = s.submission_id '
            query += 'AND s.benchmark_id = b.benchmark_id'
            row = self.con.execute(query, (run_id,)).fetchone()
            if not row['result_schema'] is None:
                schema = ResultSchema.from_dict(json.loads(row['result_schema']))
                fh = state.files[schema.result_file_id]
                results = util.read_object(fh.filename)
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
                inssql = 'INSERT INTO {}({}) VALUES({})'.format(
                     run.result_table(),
                    ','.join(columns),
                    ','.join(['?'] * len(columns))
                )
                stmts.append((inssql, values))
        # Execute all SQL statements
        for sql, values in stmts:
            self.con.execute(sql, values)
        self.con.commit()
        # Return run handle with the updated state
        return RunHandle(
            identifier=run_id,
            submission_id=run.submission_id,
            benchmark_id=run.benchmark_id,
            state=state,
            arguments=run.arguments
        )


# -- Helper classes ------------------------------------------------------------

class ArgumentEncoder(json.JSONEncoder):
    """JSON encoder for run argument values. The encoder is required to handle
    argument values that are input file objects.
    """
    def default(self, obj):
        if isinstance(obj, InputFile):
            return {
                'fileHandle': {
                    'filepath': obj.filepath,
                    'identifier': obj.identifier,
                    'filename': obj.file_name,
                },
                'targetPath': obj.target_path
            }
        # Let the base class default method raise the TypeError
            return json.JSONEncoder.default(self, obj)
