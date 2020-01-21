# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Submissions are used to (1) define groups of users that participate together
as a team in a benchmark, and (2) group the different runs (using different
parameters for example) that teams execute as part of their participation in
a benchmark.

Input files that are uploaded by the user for workflow rons are under the
controll of the submission manager. File are stored on disk in separate
directories for the respective submissions.
"""

import json
import mimetypes
import os
import shutil

from flowserv.model.user.base import UserHandle
from flowserv.core.files import FileHandle
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.template.schema import ResultSchema

import flowserv.core.error as err
import flowserv.model.constraint as constraint
import flowserv.model.ranking as ranking
import flowserv.model.parameter.util as pd
import flowserv.core.util as util


class SubmissionHandle(object):
    """A submission represents a user-specific version of a benchmark template.
    Submissions group users that participate in a benchmark as well as the
    workflow runs that are executed by those users.

    Submissions have unique identifier and names that are used to identify them
    internally and externally. Each submission has an owner and a list of team
    memebers.

    FOr each submission the users can define additional input parameters that
    are required by their submission code. Threfore, the submission maintains
    a modified copy of the benchmark template.

    Maintains an optional reference to the submission manager that allows to
    load submission members and runs on-demand.
    """
    def __init__(
        self, identifier, name, benchmark_id, owner_id, parameters,
        workflow_spec, engine, members=None, manager=None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique submission identifier
        name: string
            Unique submission name
        benchmark_id: string
            Unique benchmark identifier
        owner_id: string
            Unique identifier for the user that created the submission
        parameters: dict(string:flowserv.model.parameter.base.TemplateParameter)
            Workflow template parameter declarations
        workflow_spec: dict
            Workflow specification
        engine: flowserv.controller.engine.BenchmarkEngine
            Benchmark workflow execution engine
        members: list(flowserv.model.user.base.UserHandle)
            List of handles for team members
        manager: flowserv.model.submission.SubmissionManager, optional
            Optional reference to the submission manager
        """
        self.identifier = identifier
        self.name = name
        self.benchmark_id = benchmark_id
        self.owner_id = owner_id
        self.parameters = parameters
        self.workflow_spec = workflow_spec
        self.engine = engine
        self.members = members
        self.manager = manager

    def get_file(self, file_id):
        """Get handle for file with given identifier. Raises an error if no file
        with given identifier exists.

        Parameters
        ----------
        file_id: string
            Unique file identifier

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.UnknownFileError
        """
        return self.manager.get_file(
            submission_id=self.identifier,
            file_id=file_id
        )

    def get_members(self):
        """Get list of submission members. Loads the member list on-demand if
        it currently is None.

        Returns
        -------
        list(flowserv.model.user.base.UserHandle)
        """
        if self.members is None:
            self.members = self.manager.list_members(self.identifier)
        return self.members

    def get_results(self, order_by=None):
        """Get list of handles for all successful runs in the given submission.
        The resulting handles contain timestamp information and run results.

        Parameters
        ----------
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the schema
            default attribute is used

        Returns
        -------
        flowserv.model.ranking.ResultRanking
        """
        return self.manager.get_results(self.identifier, order_by=order_by)

    def get_runs(self):
        """Get a list of all runs for the submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(flowserv.model.workflow.run.RunHandle)
        """
        return self.manager.get_runs(self.identifier)

    def has_member(self, user_id):
        """Test if the user with the given identifier is a member of the
        submission.

        Parameters
        ----------
        user_id: string
            Unique user identifier

        Returns
        -------
        bool
        """
        for user in self.get_members():
            if user.identifier == user_id:
                return True
        return False

    def list_files(self):
        """Get list of handles for all files that have been uploaded for the
        submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(flowserv.core.files.FileHandle)
        """
        return self.manager.list_files(self.identifier)

    def reset_members(self):
        """Set member list to None if the list had been updated. This will
        ensure that submission members will be loaded on-demand.
        """
        self.members = None

    def start_run(self, arguments, template):
        """Run benchmark for the submission with the given set of arguments.
        Generates a modified workflow template using the given base template
        and the submission-specific parameter declarations and workflow
        specification.

        Parameters
        ----------
        arguments: dict(flowserv.model.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template
        template: flowserv.model.template.base.WorkflowTemplate
            Workflow template containing the parameterized specification and the
            parameter declarations

        Returns
        -------
        flowserv.model.workflow.run.RunHandle

        Raises
        ------
        flowserv.core.error.MissingArgumentError
        """
        return self.engine.start_run(
            submission_id=self.identifier,
            arguments=arguments,
            template=WorkflowTemplate(
                identifier=template.identifier,
                sourcedir=template.sourcedir,
                workflow_spec=self.workflow_spec,
                parameters=self.parameters,
                result_schema=template.result_schema,
                modules=template.modules
            )
        )


class SubmissionManager(object):
    """Manager for submissions from groups of users that participate in a
    benchmark. All information is maintained in an underlying database.

    The submission manager maintains files that are uploaded by the user for
    workflow runs. Each file is associated with exectly one submissions. Files
    are maintained in subfolders of a base directory on disk. For each
    submission a separate directory is created. Within this directory the files
    are stored using their unique identifier as the file name. The original
    file name is maintained in the underlying database.
    """
    def __init__(self, con, directory, engine):
        """Initialize the connection to the database that is used for storage.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        directory : string
            Path to the base directory for storing uploaded filee
        engine: flowserv.controller.engine.BenchmarkEngine
            Benchmark workflow execution engine
        """
        self.con = con
        # Create directory if it does not exist
        self.directory = util.create_dir(os.path.abspath(directory))
        self.engine = engine

    def create_submission(
        self, benchmark_id, name, user_id, parameters, workflow_spec,
        members=None
    ):
        """Create a new submission for a given benchmark. Within each benchmark,
        the names of submissions are expected to be unique.

        A submission may define additional parameters for a template. The full
        parameter list is stored with the submission as well as the workflow
        specification.

        A submission may have a list of users that are submission members which
        allows them to submit runs. The user that creates the submission, i.e.,
        the user identified by user_id is always part of the list of submission
        members.

        If a list of members is given it is ensured that each identifier in the
        list references an existing user.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        name: string
            Submission name
        user_id: string
            Unique identifier of the user that created the submission
        parameters: list(flowserv.model.parameter.base.TemplateParameter)
            List of workflow template parameter declarations for the submission
        workflow_spec: dict
            Workflow specification
        members: list(string), optional
            Optional list of user identifiers for other sumbission members

        Returns
        -------
        flowserv.model.submission.SubmissionHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownBenchmarkError
        flowserv.core.error.UnknownUserError
        """
        # Ensure that the benchmark exists
        sql = 'SELECT * FROM benchmark WHERE benchmark_id = ?'
        if self.con.execute(sql, (benchmark_id,)).fetchone() is None:
            raise err.UnknownBenchmarkError(benchmark_id)
        # Ensure that the given name is valid and unique for the benchmark
        sql = 'SELECT name FROM benchmark_submission '
        sql += 'WHERE benchmark_id = ? AND name = ?'
        args = (benchmark_id, name)
        constraint.validate_name(name, con=self.con, sql=sql, args=args)
        # Ensure that all users in the given member list exist
        if members is not None:
            sql = 'SELECT user_id FROM api_user WHERE user_id = ?'
            for member_id in members:
                if self.con.execute(sql, (member_id,)).fetchone() is None:
                    raise err.UnknownUserError(member_id)
        # Create a unique identifier for the new submission and the submission
        # upload directory
        identifier = util.get_unique_identifier()
        util.create_dir(os.path.join(self.directory, identifier))
        # Add owner to list of initial members
        if members is None:
            members = set([user_id])
        else:
            members = set(members)
            if user_id not in members:
                members.add(user_id)
        # Enter submission information into database and commit all changes
        sql = (
            'INSERT INTO benchmark_submission(submission_id, benchmark_id, '
            'name, owner_id, parameters, workflow_spec'
            ') VALUES(?, ?, ?, ?, ?, ?)'
        )
        values = (
            identifier,
            benchmark_id,
            name,
            user_id,
            json.dumps([p.to_dict() for p in parameters.values()]),
            json.dumps(workflow_spec)
        )
        self.con.execute(sql, values)
        sql = (
            'INSERT INTO submission_member(submission_id, user_id) '
            'VALUES(?, ?)'
        )
        for member_id in members:
            self.con.execute(sql, (identifier, member_id))
        self.con.commit()
        # Return the created submission object
        return self.get_submission(identifier)

    def delete_file(self, submission_id, file_id):
        """Delete file with given identifier. Raises an error if the file does
        not exist.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file_id: string
            Unique file identifier

        Raises
        ------
        flowserv.core.error.UnknownFileError
        """
        # Get the file handle which contains the path to the file on disk.
        # This will raise an error if the file does not exist
        fh = self.get_file(submission_id=submission_id, file_id=file_id)
        fh.delete()
        sql = 'DELETE FROM submission_file WHERE file_id = ?'
        self.con.execute(sql, (file_id,))
        self.con.commit()

    def delete_submission(self, submission_id, commit_changes=True):
        """Delete the entry for the given submission from the underlying
        database. The method will delete the directory that contains the
        uploaded files for the submission which potentially deletes all
        associated run result files as well.

        The changes to the underlying database are only commited if the
        commit_changes flag is True. The deletion of files and directories
        cannot be rolled back.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        commit_changes: bool, optional
            Commit all changes to the database if True

        Raises
        ------
        flowserv.core.error.InvalidRunStateError
        flowserv.core.error.UnknownSubmissionError
        """
        # Get submission handle to ensure that the submission exists
        self.get_submission(submission_id, load_members=False)
        # Get a list of submission runs to delete them individually
        sql = 'SELECT run_id FROM benchmark_run WHERE submission_id = ?'
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            self.engine.delete_run(row['run_id'])
        # Create DELETE statements for all tables that may contain information
        # about the submission.
        psql = 'DELETE FROM {} WHERE submission_id = ?'
        stmts = list()
        stmts.append(psql.format('submission_member'))
        stmts.append(psql.format('submission_file'))
        stmts.append(psql.format('benchmark_submission'))
        for sql in stmts:
            self.con.execute(sql, (submission_id,))
        # Commit changes only of the respective flag is True
        if commit_changes:
            self.con.commit()
        # Delete the base directory for the submission.
        shutil.rmtree(os.path.join(self.directory, submission_id))

    def get_file(self, submission_id, file_id):
        """Get handle for file with given identifier. Raises an error if no file
        with given identifier exists.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.UnknownFileError
        """
        # Retrieve file information from disk. Use both identifier to ensure
        # that the file belongs to the submission. Raise error if the file
        # does not exist.
        sql = (
            'SELECT name, mimetype FROM submission_file '
            'WHERE submission_id = ? AND file_id = ?'
        )
        row = self.con.execute(sql, (submission_id, file_id)).fetchone()
        if row is None:
            raise err.UnknownFileError(file_id)
        return FileHandle(
            identifier=file_id,
            file_name=row['name'],
            filepath=os.path.join(self.directory, submission_id, file_id),
            mimetype=row['mimetype']
        )

    def get_results(self, submission_id, order_by=None):
        """Get list of handles for all successful runs in the given submission.
        The result handles contain timestamp information and run results.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the
            schema default attribute is used

        Returns
        -------
        flowserv.model.ranking.ResultRanking

        Raises
        ------
        flowserv.core.error.UnknownSubmissionError
        """
        # Get result schema for the associated benchmark. Raise error if the
        # given submission identifier is invalid.
        sql = 'SELECT b.benchmark_id, b.result_schema '
        sql += 'FROM benchmark b, benchmark_submission s '
        sql += 'WHERE b.benchmark_id = s.benchmark_id AND s.submission_id = ?'
        row = self.con.execute(sql, (submission_id,)).fetchone()
        if row is None:
            raise err.UnknownSubmissionError(submission_id)
        # Get the result schema as defined in the workflow template
        if not row['result_schema'] is None:
            schema = ResultSchema.from_dict(json.loads(row['result_schema']))
        else:
            schema = ResultSchema()
        return ranking.query(
            con=self.con,
            benchmark_id=row['benchmark_id'],
            schema=schema,
            filter_stmt='s.submission_id = ?',
            args=(submission_id,),
            order_by=order_by,
            include_all=True
        )

    def get_runs(self, submission_id):
        """Get a list of all runs for the given submission.

        This method does not check if the submission exists. Thie method is
        primarily intended to be called by the submission handle to load runs
        on-demand. Existence of the submission is expected to have been
        verified when the submission handle was created.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(flowserv.model.workflow.run.RunHandle)
        """
        return self.engine.list_runs(submission_id)

    def get_submission(self, submission_id, load_members=True):
        """Get handle for submission with the given identifier. Submission
        members may be loaded on-demand for performance reasons.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        load_members: bool, optional
            Load handles for submission members if True

        Returns
        -------
        flowserv.model.submission.SubmissionHandle

        Raises
        ------
        flowserv.core.error.UnknownSubmissionError
        """
        # Get submission information. Raise error if the identifier is unknown.
        sql = 'SELECT name, benchmark_id, owner_id, parameters, workflow_spec '
        sql += 'FROM benchmark_submission '
        sql += 'WHERE submission_id = ?'
        row = self.con.execute(sql, (submission_id,)).fetchone()
        if row is None:
            raise err.UnknownSubmissionError(submission_id)
        name = row['name']
        benchmark_id = row['benchmark_id']
        owner_id = row['owner_id']
        parameters = pd.create_parameter_index(
            json.loads(row['parameters']),
            validate=False
        )
        workflow_spec = json.loads(row['workflow_spec'])
        # Get list of team members (only of load flag is True)
        if load_members:
            members = self.list_members(submission_id)
        else:
            members = None
        # Return the submission handle
        return SubmissionHandle(
            identifier=submission_id,
            name=name,
            benchmark_id=benchmark_id,
            owner_id=owner_id,
            parameters=parameters,
            workflow_spec=workflow_spec,
            engine=self.engine,
            members=members,
            manager=self
        )

    def list_files(self, submission_id):
        """Get list of file handles for all files that have been uploaded to a
        given submission.

        This method does not check if the submission exists. Thie method is
        primarily intended to be called by the submission handle to load
        submission members on-demand. Existence of the submission is expected
        to have been verified when the submission handle was created.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(flowserv.core.files.FileHandle)
        """
        # Create the result set from a SQL query of the submission file table
        sql = (
            'SELECT file_id, name, mimetype FROM submission_file '
            'WHERE submission_id = ?'
        )
        rs = list()
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            file_id = row['file_id']
            fh = FileHandle(
                identifier=file_id,
                file_name=row['name'],
                filepath=os.path.join(self.directory, submission_id, file_id),
                mimetype=row['mimetype']
            )
            rs.append(fh)
        return rs

    def list_members(self, submission_id):
        """Get a list of all users that are member of the given submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        raise_error: bool, optional
            Raise error if true and the member list is empty

        Returns
        -------
        list(flowserv.model.user.base.UserHandle)
        """
        members = list()
        sql = 'SELECT s.user_id, u.name '
        sql += 'FROM submission_member s, api_user u '
        sql += 'WHERE s.user_id = u.user_id AND s.submission_id = ?'
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            user = UserHandle(identifier=row['user_id'], name=row['name'])
            members.append(user)
        return members

    def list_submissions(self, benchmark_id=None, user_id=None):
        """Get a listing of submission descriptors. If the user is given only
        those submissions are returned that the user is a member of. If the
        benchmark identifier is given only submissions for the given benchmark
        are included.

        Parameters
        ----------
        benchmark_id: string, optional
            Unique benchmark identifier
        user_id: string, optional
            Unique user identifier

        Returns
        -------
        list(flowserv.model.submission.SubmissionHandle)
        """
        # Generate SQL query depending on whether the user is given or not
        sql = 'SELECT s.submission_id, s.name, s.benchmark_id, s.owner_id, '
        sql += 's.parameters, s.workflow_spec '
        sql += 'FROM benchmark_submission s'
        para = list()
        if user_id is not None:
            sql += ' WHERE s.submission_id IN ('
            sql += 'SELECT m.submission_id '
            sql += 'FROM submission_member m '
            sql += 'WHERE m.user_id = ?)'
            para.append(user_id)
        if benchmark_id is not None:
            if user_id is None:
                sql += ' WHERE '
            else:
                sql += ' AND '
            sql += 's.benchmark_id = ?'
            para.append(benchmark_id)
        # Create list of submission handle from query result
        submissions = list()
        for row in self.con.execute(sql, para).fetchall():
            parameters = pd.create_parameter_index(
                json.loads(row['parameters']),
                validate=False
            )
            s = SubmissionHandle(
                identifier=row['submission_id'],
                name=row['name'],
                benchmark_id=row['benchmark_id'],
                owner_id=row['owner_id'],
                parameters=parameters,
                workflow_spec=json.loads(row['workflow_spec']),
                engine=self.engine,
                manager=self
            )
            submissions.append(s)
        return submissions

    def update_submission(self, submission_id, name=None, members=None):
        """Update the name and/or list of members for a submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        name: string, optional
            Unique user identifier
        members: list(string), optional
            List of user identifier for submission members

        Returns
        -------
        flowserv.model.submission.SubmissionHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownSubmissionError
        """
        # Get submission handle. This will raise an error if the submission is
        # unknown.
        submission = self.get_submission(submission_id)
        # If name and members are None we simply return the submission handle.
        if name is None and members is None:
            return submission
        if name is not None and name is not submission.name:
            # Ensure that the given name is valid and unique for the benchmark
            sql = 'SELECT name FROM benchmark_submission '
            sql += 'WHERE benchmark_id = ? AND name = ?'
            args = (submission.benchmark_id, name)
            constraint.validate_name(name, con=self.con, sql=sql, args=args)
            sql = 'UPDATE benchmark_submission SET name = ? '
            sql += 'WHERE submission_id = ?'
            self.con.execute(sql, (name, submission_id))
            submission.name = name
        if members is not None:
            # Delete members that are not in the given list
            sql = 'DELETE FROM submission_member '
            sql += 'WHERE submission_id = ? AND user_id = ?'
            for user in submission.get_members():
                if user.identifier not in members:
                    self.con.execute(sql, (submission_id, user.identifier))
            # Add users that are not members of the submission
            sql = 'INSERT INTO submission_member(submission_id, user_id) '
            sql += 'VALUES(?, ?)'
            for user_id in members:
                if not submission.has_member(user_id):
                    self.con.execute(sql, (submission_id, user_id))
            # Clear the member list in the submission handle to ensure that the
            # members are reloaded on-demand
            submission.reset_members()
        self.con.commit()
        return submission

    def upload_file(self, submission_id, file, file_name, file_type=None):
        """Create a new entry from a given file stream. Will copy the given
        file to a file in the base directory.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file: werkzeug.datastructures.FileStorage
            File object (e.g., uploaded via HTTP request)
        file_name: string
            Name of the file
        file_type: string, optional
            Identifier for the file type (e.g., the file MimeType). This could
            also by the identifier of a content handler.

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownSubmissionError
        """
        # Ensure that the given file name is valid
        constraint.validate_name(file_name)
        # Get submission handle. These steps ensure that the submission and the
        # directory for submission uploads exists.
        self.get_submission(submission_id, load_members=False)
        file_dir = util.create_dir(os.path.join(self.directory, submission_id))
        # Get the mime-type of the uploaded file. At this point we assume that
        # the file_type either contains the mime-type or that it is None. In
        # the latter case the mime-type is guessed from the name of the
        # uploaded file (may be None).
        if file_type is not None:
            mimetype = file_type
        else:
            mimetype, _ = mimetypes.guess_type(url=file_name)
        # Create a new unique identifier for the file and save the file object
        # to the new file path.
        identifier = util.get_unique_identifier()
        output_file = os.path.join(file_dir, identifier)
        file.save(output_file)
        # Insert information into database
        sql = (
            'INSERT INTO submission_file'
            '(submission_id, file_id, file_type, mimetype, name) '
            'VALUES(?, ?, ?, ?, ?)'
        )
        params = (submission_id, identifier, file_type, mimetype, file_name)
        self.con.execute(sql, params)
        self.con.commit()
        # Return handle to uploaded file
        return FileHandle(
            identifier=identifier,
            filepath=output_file,
            file_name=file_name,
            mimetype=mimetype
        )
