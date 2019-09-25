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

from robapi.model.run.result import ResultRanking
from robapi.model.run.base import RunHandle
from robcore.model.user.base import UserHandle
from robcore.io.files import FileHandle
from robcore.model.template.schema import ResultSchema

import robcore.error as err
import robcore.model.constraint as constraint
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
        manager: robapi.model.submission.SubmissionManager, optional
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
        robapi.model.run.result.ResultRanking

        Raises
        ------
        robcore.error.UnknownSubmissionError
        """
        return self.manager.get_results(self.identifier, order_by=order_by)

    def get_runs(self):
        """Get list of handles for all runs in the submission.

        Returns
        -------
        list(robapi.model.run.base.RunHandle)

        Raises
        ------
        robcore.error.UnknownSubmissionError
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
    def __init__(self, con, directory):
        """Initialize the connection to the database that is used for storage.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        directory : string
            Path to the base directory for storing uploaded files
        """
        self.con = con
        # Create directory if it does not exist
        self.directory = util.create_dir(os.path.abspath(directory))

    def add_member(self, submission_id, user_id):
        """Add a user as member to an existing submission. If the user already
        is a member of the submission an error is raised.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user_id: string
            Unique user identifier

        Raises
        ------
        robcore.error.ConstraintViolationError
        """
        sql = 'INSERT INTO submission_member(submission_id, user_id) VALUES(?, ?)'
        try:
            self.con.execute(sql, (submission_id, user_id))
            self.con.commit()
        except Exception:
            # Depending on the database system that is being used the type of
            # the exception may differ. Here we assume that any exception is
            # due to a primary key violation (i.e., the user is already a
            # member of the submission)
            msg = '{} already member of {}'.format(submission_id, user_id)
            raise err.ConstraintViolationError(msg)

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
        robapi.model.submission.SubmissionHandle

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

    def delete_submission(self, submission_id):
        """Delete the entry for the given submission from the underlying
        database. Note that this will also remove any runs and run results that
        are associated with the submission as well as any associated file
        resources.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Raises
        ------
        robcore.error.UnknownSubmissionError
        """
        # Get the handles for all associated submission runs and collect the
        # file resources that are associated with each of them. This will raise
        # an error if the submission is unknown.
        filenames = list()
        for run in self.get_runs(submission_id):
            if run.is_success():
                for fh in run.get_files():
                    filenames.append(fh.filename)
        # Start by deleting all rows in the database that belong to the
        # submission. Delete files after the database changes are committed
        # since there is no rollback option for file deletes.
        psql = 'DELETE FROM {} WHERE run_id IN ('
        psql += 'SELECT run_id FROM benchmark_run WHERE submission_id = ?)'
        stmts = list()
        stmts.append(psql.format('run_result_file'))
        stmts.append(psql.format('run_error_log'))
        psql = 'DELETE FROM {} WHERE submission_id = ?'
        stmts.append(psql.format('benchmark_run'))
        stmts.append(psql.format('submission_member'))
        stmts.append(psql.format('submission_file'))
        stmts.append(psql.format('benchmark_submission'))
        for sql in stmts:
            self.con.execute(sql, (submission_id,))
        self.con.commit()
        # Delete all file resources
        for f in filenames:
            # Don't raise an error if the file does not exist or cannot be
            # removed
            try:
                os.remove(f)
            except OSError:
                pass

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
        robapi.model.run.result.ResultRanking

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
        return ResultRanking.query(
            con=self.con,
            benchmark_id=row['benchmark_id'],
            schema=schema,
            filter_stmt='s.submission_id = ?',
            args=(submission_id,),
            order_by=order_by,
            include_all=True
        )

    def get_runs(self, submission_id):
        """Get list of handles for all runs in the given submission. All run
        information is read from the underlying database. This method does
        not query the backend to get workflow states.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(robapi.model.run.base.RunHandle)

        Raises
        ------
        robcore.error.UnknownSubmissionError
        """
        # Get submission handle to ensure that the submission exists. this will
        # raise an error if the submission does not exist.
        self.get_submission(submission_id, load_members=False)
        # Fetch run information from the database and return list of run
        # handles.
        sql = 'SELECT r.run_id, s.benchmark_id, s.submission_id, r.state, '
        sql += 'r.arguments, r.created_at, r.started_at, r.ended_at '
        sql += 'FROM benchmark b, benchmark_submission s, benchmark_run r '
        sql += 'WHERE s.submission_id = r.submission_id AND r.submission_id = ? '
        sql += 'ORDER BY r.created_at'
        result = list()
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            result.append(RunHandle.from_db(doc=row, con=self.con))
        return result

    def get_submission(self, submission_id, load_members=True):
        """Get handle for submission with the given identifier. Submission
        members may be loaded on-demand for performance reasons.

        Parameters
        ----------
        identifier: string
            Unique submission identifier
        load_members: bool, optional
            Load handles for submission members if True

        Returns
        -------
        robapi.model.submission.SubmissionHandle

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

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        list(robcore.io.files.FileHandle)

        Raises
        ------
        robcore.error.UnknownSubmissionError
        """
        # Get submission handle to ensure that the submission exists. this will
        # raise an error if the submission does not exist.
        self.get_submission(submission_id, load_members=False)
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

    def list_members(self, submission_id, raise_error=True):
        """Get a list of all users that are member of the given submission.
        Raises an unknown submission error if the list of members is empty and
        the raise error flag is true.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        raise_error: bool, optional
            Raise error if true and the member list is empty

        Returns
        -------
        list(robcore.model.user.base.UserHandle)

        Raises
        ------
        robcore.error.UnknownSubmissionError
        """
        members = list()
        sql = 'SELECT s.user_id, u.name '
        sql += 'FROM submission_member s, api_user u '
        sql += 'WHERE s.user_id = u.user_id AND s.submission_id = ?'
        for row in self.con.execute(sql, (submission_id,)).fetchall():
            user = UserHandle(identifier=row['user_id'], name=row['name'])
            members.append(user)
        if len(members) == 0 and raise_error:
            raise err.UnknownSubmissionError(submission_id)
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
        list(robapi.model.submission.SubmissionHandle)
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

    def remove_member(self, submission_id, user_id):
        """Remove a user as a menber from the given submission. The return value
        indicates if the submission exists and the user was a memmer of that
        submission.

        There are currently not constraints enforced, i.e., any user can be
        removed as submission member, even the submission owner.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        user_id: string
            Unique user identifier

        Returns
        -------
        bool
        """
        cur = self.con.cursor()
        sql = 'DELETE FROM submission_member '
        sql += 'WHERE submission_id = ? AND user_id = ?'
        # Use row count to determine if the submission existed or not
        rowcount = cur.execute(sql, (submission_id, user_id)).rowcount
        self.con.commit()
        return rowcount > 0

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
        robapi.model.submission.SubmissionHandle

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
        robcore.error.UnknownSubmissionError
        """
        # Ensure that the given file name is valid
        constraint.validate_name(file_name)
        # Get submission handle to ensure that the submission exists. this will
        # raise an error if the submission does not exist.
        self.get_submission(submission_id, load_members=False)
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
