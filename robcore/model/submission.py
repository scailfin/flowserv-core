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
import os
import shutil

from robcore.model.user.base import UserHandle
from robcore.io.files import FileHandle
from robcore.model.template.schema import ResultSchema

import robcore.error as err
import robcore.model.constraint as constraint
import robcore.model.ranking as ranking
import robcore.util as util


class SubmissionHandle(object):
    """A submission is a set of workflow runs that are submmited by a group of
    users that participate in a benchmark.

    Submissions have unique identifier and names that are used to identify them
    internally and externally. Each submission has an owner and a list of team
    memebers.

    Maintains an optional reference to the submission manager that allows to
    load submission members and runs on-demand.
    """
    def __init__(
        self, identifier, name, benchmark_id, owner_id, members=None,
        manager=None
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
        members: list(robcore.model.user.base.UserHandle)
            List of handles for team members
        manager: robcore.model.submission.SubmissionManager, optional
            Optional reference to the submission manager
        """
        self.identifier = identifier
        self.name = name
        self.benchmark_id = benchmark_id
        self.owner_id = owner_id
        self.members = members
        self.manager = manager

    def get_members(self):
        """Get list of submission members. Loads the member list on-demand if
        it currently is None.

        Returns
        -------
        list(robcore.model.user.base.UserHandle)
        """
        if self.members is None:
            self.members = self.manager.list_members(
                submission_id=self.identifier,
                raise_error=False
            )
        return self.members

    def get_results(self, order_by=None):
        """Get list of handles for all successful runs in the given submission.
        The resulting handles contain timestamp information and run results.

        Parameters
        ----------
        order_by: list(robcore.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the schema
            default attribute is used

        Returns
        -------
        robcore.model.ranking.ResultRanking
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
        list(robcore.model.workflow.run.RunHandle)
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

    def reset_members(self):
        """Set member list to None if the list had been updated. This will
        ensure that submission members will be loaded on-demand.
        """
        self.members = None

    def start_run(self, arguments):
        """Run benchmark for the submission with the given set of arguments.

        Parameters
        ----------
        arguments: dict(benchtmpl.workflow.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        robcore.model.workflow.run.RunHandle

        Raises
        ------
        robcore.error.MissingArgumentError
        """
        self.manager.start_run(
            submission_id=self.identifier,
            arguments=arguments
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
        engine: robcore.model.workflow.engine.BenchmarkEngine
            Benchmark workflow execution engine
        """
        self.con = con
        # Create directory if it does not exist
        self.directory = util.create_dir(os.path.abspath(directory))
        self.engine = engine

    def create_submission(self, benchmark_id, name, user_id, members=None):
        """Create a new submission for a given benchmark. Within each benchmark,
        the names of submissions are expected to be unique.

        A submission may have a list of users that are submission members which
        allows them to submit runs. The user that creates the submission, i.e.,
        the user identified by user_id is always part of the list of submission
        members.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        name: string
            Submission name
        user_id: string
            Unique identifier of the user that created the submission
        members: list(string), optional
            Optional list of user identifiers for other sumbission members

        Returns
        -------
        robcore.model.submission.SubmissionHandle

        Raises
        ------
        robcore.error.ConstraintViolationError
        robcore.error.UnknownBenchmarkError
        """
        # Ensure that the benchmark exists
        sql = 'SELECT * FROM benchmark WHERE benchmark_id = ?'
        if self.con.execute(sql, (benchmark_id,)).fetchone() is None:
            raise err.UnknownBenchmarkError(benchmark_id)
        # Ensure that the given name is valid and unique for the benchmark
        sql = 'SELECT name FROM benchmark_submission '
        sql += 'WHERE benchmark_id = \'{}\' AND name = ?'.format(benchmark_id)
        constraint.validate_name(name, con=self.con, sql=sql)
        # Create a new instance of the sumbission class.
        identifier = util.get_unique_identifier()
        # Add owner to list of initial members
        if members is None:
            members = list([user_id])
        elif not user_id in members:
            members.append(user_id)
        # Enter submission information into database and commit all changes
        sql = 'INSERT INTO benchmark_submission('
        sql += 'submission_id, benchmark_id, name, owner_id'
        sql += ') VALUES(?, ?, ?, ?)'
        values = (identifier, benchmark_id, name, user_id)
        self.con.execute(sql, values)
        sql = 'INSERT INTO submission_member(submission_id, user_id) VALUES(?, ?)'
        for member_id in set(members):
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
        robcore.error.UnknownFileError
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
        robcore.error.InvalidRunStateError
        """
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
        """Get handle for file with given identifier. Returns None if no file
        with given identifier exists.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        robcore.io.files.FileHandle

        Raises
        ------
        robcore.error.UnknownFileError
        """
        # Retrieve file information from disk. Use both identifier to ensure
        # that the file belongs to the submission. Raise error if the file
        # does not exist.
        sql = 'SELECT name FROM submission_file '
        sql += 'WHERE submission_id = ? AND file_id = ?'
        row = self.con.execute(sql, (submission_id, file_id)).fetchone()
        if row is None:
            raise err.UnknownFileError(file_id)
        return FileHandle(
            identifier=file_id,
            file_name=row['name'],
            filepath=os.path.join(self.directory, submission_id, file_id)
        )

    def get_results(self, submission_id, order_by=None):
        """Get list of handles for all successful runs in the given submission.
        The result handles contain timestamp information and run results.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        order_by: list(robcore.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the schema
            default attribute is used

        Returns
        -------
        robcore.model.ranking.ResultRanking

        Raises
        ------
        robcore.error.UnknownSubmissionError
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
        on-demand. Existence of the submission is expected to have been verified
        when the submission handle was created.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(robcore.model.workflow.run.RunHandle)
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
        robcore.model.submission.SubmissionHandle

        Raises
        ------
        robcore.error.UnknownSubmissionError
        """
        # Get submission information. Raise error if the identifier is unknown.
        sql = 'SELECT name, benchmark_id, owner_id '
        sql += 'FROM benchmark_submission '
        sql += 'WHERE submission_id = ?'
        row = self.con.execute(sql, (submission_id,)).fetchone()
        if row is None:
            raise err.UnknownSubmissionError(submission_id)
        name = row['name']
        benchmark_id = row['benchmark_id']
        owner_id = row['owner_id']
        # Get list of team members (only of load flag is True)
        if load_members:
            members = self.list_members(submission_id, raise_error=False)
        else:
            members = None
        # Return the submission handle
        return SubmissionHandle(
            identifier=submission_id,
            name=name,
            benchmark_id=benchmark_id,
            owner_id=owner_id,
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
        list(robcore.io.files.FileHandle)
        """
        # Create the result set from a SQL query of the submission file table
        sql = 'SELECT file_id, name FROM submission_file '
        sql += 'WHERE submission_id = ?'
        rs = list()
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            file_id = row['file_id']
            fh = FileHandle(
                identifier=file_id,
                file_name=row['name'],
                filepath=os.path.join(self.directory, submission_id, file_id)
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
        list(robcore.model.user.base.UserHandle)
        """
        members = list()
        sql = 'SELECT s.user_id, u.name '
        sql += 'FROM submission_member s, api_user u '
        sql += 'WHERE s.user_id = u.user_id AND s.submission_id = ?'
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            user = UserHandle(identifier=row['user_id'], name=row['name'])
            members.append(user)
        return members

    def list_submissions(self, user=None):
        """Get a listing of submission descriptors. If the user is given only
        those submissions are returned that the user is a member of.

        Parameters
        ----------
        user: robcore.model.user.base.UserHandle, optional
            User handle

        Returns
        -------
        list(robcore.model.submission.SubmissionHandle)
        """
        # Generate SQL query depending on whether the user is given or not
        sql = 'SELECT s.submission_id, s.name, s.benchmark_id, s.owner_id '
        sql += 'FROM benchmark_submission s'
        if user is None:
            para = ()
        else:
            sql += ' WHERE s.submission_id IN ('
            sql += 'SELECT m.submission_id '
            sql += 'FROM submission_member m '
            sql += 'WHERE m.user_id = ?)'
            para = (user.identifier,)
        # Create list of submission handle from query result
        submissions = list()
        for row in self.con.execute(sql, para).fetchall():
            s = SubmissionHandle(
                identifier=row['submission_id'],
                name=row['name'],
                benchmark_id=row['benchmark_id'],
                owner_id=row['owner_id'],
                manager=self
            )
            submissions.append(s)
        return submissions

    def start_run(self, submission_id, arguments):
        """Run benchmark for a given submission with the given set of arguments.

        This method does not check if the submission exists. Thie method is
        primarily intended to be called by the submission handle to start runs.
        Existence of the submission is expected to have been verified when the
        submission handle was created.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        arguments: dict(benchtmpl.workflow.parameter.value.TemplateArgument)
            Dictionary of argument values for parameters in the template

        Returns
        -------
        robcore.model.workflow.run.RunHandle

        Raises
        ------
        robcore.error.MissingArgumentError
        """
        return self.engine.start_run(
            submission_id=submission_id,
            arguments=arguments
        )

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
        robcore.model.submission.SubmissionHandle

        Raises
        ------
        robcore.error.ConstraintViolationError
        robcore.error.UnknownSubmissionError
        """
        # Get submission handle. This will raise an error if the submission is
        # unknown.
        submission = self.get_submission(submission_id)
        # If name and members are None we simply return the submission handle.
        if name is None and members is None:
            return submission
        if not name is None and name != submission.name:
            # Ensure that the given name is valid and unique for the benchmark
            sql = 'SELECT name FROM benchmark_submission '
            sql += 'WHERE benchmark_id = \'{}\' AND name = ?'
            sql = sql.format(submission.benchmark_id)
            constraint.validate_name(name, con=self.con, sql=sql)
            sql = 'UPDATE benchmark_submission SET name = ? '
            sql += 'WHERE submission_id = ?'
            self.con.execute(sql, (name, submission_id))
            submission.name = name
        if not members is None:
            # Delete members that are not in the given list
            sql = 'DELETE FROM submission_member '
            sql += 'WHERE submission_id = ? AND user_id = ?'
            for user in submission.get_members():
                if not user.identifier in members:
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

    def upload_file(self, submission_id, file, file_name):
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

        Returns
        -------
        robcore.io.files.FileHandle

        Raises
        ------
        robcore.error.ConstraintViolationError
        """
        # Ensure that the given file name is valid
        constraint.validate_name(file_name)
        # Ensure that the directory for submission uploads exists
        file_dir = util.create_dir(os.path.join(self.directory, submission_id))
        # Create a new unique identifier for the file.
        identifier = util.get_unique_identifier()
        # Save the file object to the new file path
        output_file = os.path.join(file_dir, identifier)
        file.save(output_file)
        # Insert information into database
        sql = 'INSERT INTO submission_file(submission_id, file_id, name) '
        sql += 'VALUES(?, ?, ?)'
        self.con.execute(sql, (submission_id, identifier, file_name))
        self.con.commit()
        # Return handle to uploaded file
        return FileHandle(
            identifier=identifier,
            filepath=output_file,
            file_name=file_name
        )
