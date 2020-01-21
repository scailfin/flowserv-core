# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The benchmark repository maintains information about benchmarks. For each
benchmark basic information is stored in the underlying database, together with
the workflow template and the result files of individual workflow runs.
"""

import json
import mimetypes
import os

from flowserv.core.files import FileHandle
from flowserv.model.template.benchmark import BenchmarkHandle
from flowserv.model.template.schema import ResultSchema

import flowserv.core.error as err
import flowserv.model.constraint as constraint
import flowserv.model.ranking as ranking
import flowserv.model.parameter.declaration as pd
import flowserv.core.util as util


class BenchmarkRepository(object):
    """The repository maintains benchmarks as well as the results of benchmark
    runs.
    """
    def __init__(self, con, template_repo, resource_basedir):
        """Initialize the database connection and the template store.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        template_store: flowserv.model.template.repo.base.TemplateRepository, optional
            Repository for workflow templates
        resource_basedir: string
            Path to the base directory that contains post-processing results
            for all benchmarks
        """
        self.con = con
        self.template_repo = template_repo
        # Create the resource directory if it does not exist
        self.resource_basedir = util.create_dir(resource_basedir, abs=True)

    def add_benchmark(
        self, name, description=None, instructions=None, sourcedir=None,
        repourl=None, specfile=None
    ):
        """Add a benchmark to the repository. The associated workflow template
        is created in the template repository from either the given source
        directory or Git repository. The template repository will raise an
        error if neither or both arguments are given.

        Each benchmark has a name and an optional description and set of
        instructions.

        Raises an error if the given benchmark name is not unique.

        Parameters
        ----------
        name: string
            Unique benchmark headline name
        description: string, optional
            Optional short description for display in benchmark listings
        instructions: string, optional
            Text containing detailed instructions for benchmark participants
        sourcedir: string, optional
            Directory containing the benchmark components, i.e., the fixed
            files and the template specification (optional).
        repourl: string, optional
            Git repository that contains the the benchmark components
        specfile: string, optional
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)

        Returns
        -------
        flowserv.model.template.benchmark.BenchmarkHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.InvalidTemplateError
        ValueError
        """
        # Ensure that the benchmark name is not empty, not longer than 512
        # character and unique.
        sql = 'SELECT name FROM benchmark WHERE name = ?'
        constraint.validate_name(name, con=self.con, sql=sql)
        # Create the workflow template in the associated template repository
        template = self.template_repo.add_template(
            sourcedir=sourcedir,
            repourl=repourl,
            specfile=specfile
        )
        t_id = template.identifier
        # Create the result table in the underlying database if the template
        # contains a schema definition
        if template.has_schema():
            schema = template.get_schema()
            ranking.create_result_table(
                con=self.con,
                benchmark_id=t_id,
                schema=schema,
                commit_changes=False
            )
            result_schema = json.dumps(schema.to_dict())
        else:
            result_schema = None
        # Get serialization of the post-processing task
        pp_task = None
        if template.postproc_task is not None:
            pp_task = json.dumps(template.postproc_task.to_dict())
        # Insert benchmark into database and return descriptor
        sql = (
            'INSERT INTO benchmark'
            '(benchmark_id, name, description, instructions, postproc_task, '
            'result_schema, static_dir, resources_dir) '
            'VALUES(?, ?, ?, ?, ?, ?, ?, ?)'
        )
        values = (
            t_id,
            name,
            description,
            instructions,
            pp_task,
            result_schema,
            template.sourcedir,
            util.create_dir(os.path.join(self.resource_basedir, t_id))
        )
        self.con.execute(sql, values)
        # Commit all changes and return the benchmark descriptor
        self.con.commit()
        return BenchmarkHandle(
            identifier=t_id,
            name=name,
            description=description,
            instructions=instructions,
            template=template,
            repo=self
        )

    def delete_benchmark(self, benchmark_id):
        """Delete the benchmark with the given identifier.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        """
        # Delete the workflow template.
        result = self.template_repo.delete_template(benchmark_id)
        # Delete the benchmark record. Use rowcount to determine if the record
        # existed.
        sql = 'DELETE FROM benchmark WHERE benchmark_id = ?'
        self.con.execute(sql, (benchmark_id,))
        self.con.commit()

    def get_benchmark(self, benchmark_id):
        """Get descriptor for the benchmark with the given identifier. Raises
        an error if no benchmark with the identifier exists.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier

        Returns
        -------
        flowserv.model.template.benchmark.BenchmarkHandle

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
        """
        # Get benchmark information from database. If the result is empty an
        # error is raised
        sql = (
            'SELECT benchmark_id, name, description, instructions, result_id '
            'FROM benchmark '
            'WHERE benchmark_id = ?'
        )
        rs = self.con.execute(sql, (benchmark_id,)).fetchone()
        if rs is None:
            raise err.UnknownBenchmarkError(benchmark_id)
        # Return handle for benchmark descriptor. The workflow handle will be
        # loaded here and not on-demand.
        return BenchmarkHandle(
            identifier=benchmark_id,
            name=rs['name'],
            description=rs['description'],
            instructions=rs['instructions'],
            template=self.template_repo.get_template(benchmark_id),
            result_id=rs['result_id'],
            repo=self
        )

    def get_benchmark_archive(self, benchmark_id, result_id=None):
        """Get in-memory compressed tar-file containing all resource files for
        a benchmark that have been generated by the post-processing step.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        result_id: string, optional
            Unique identifier of the result run that generated the resources

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
        flowserv.core.error.UnknownResourceError
        """
        # Get benchmark information from database. If the result is empty an
        # error is raised
        sql = (
            'SELECT resources_dir, result_id '
            'FROM benchmark '
            'WHERE benchmark_id = ?'
        )
        rs = self.con.execute(sql, (benchmark_id,)).fetchone()
        if rs is None:
            raise err.UnknownBenchmarkError(benchmark_id)
        # Include all files in the result directory in the returned tar file.
        if result_id is not None:
            res_dir = os.path.join(rs['resources_dir'], result_id)
        else:
            res_dir = os.path.join(rs['resources_dir'], rs['result_id'])
        print('attach resource directory {}'.format(res_dir))
        return util.tar_gz(directory=res_dir)

    def get_benchmark_resource(self, benchmark_id, resource_id, result_id=None):
        """Get file handle for a benchmark resource that has been generated
        by the post-processing step.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        resource_id: string
            Unique resource identifier
        result_id: string, optional
            Unique identifier of the result run that generated the resources

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
        flowserv.core.error.UnknownResourceError
        """
        # Get benchmark information from database. If the result is empty an
        # error is raised
        sql = (
            'SELECT resources_dir, result_id '
            'FROM benchmark '
            'WHERE benchmark_id = ?'
        )
        rs = self.con.execute(sql, (benchmark_id,)).fetchone()
        if rs is None:
            raise err.UnknownBenchmarkError(benchmark_id)
        # Check if the requested resource file exists. If the file does not
        # exists an error is raised. In the future we may want to include the
        # post-processing declaration in above query to have access to the
        # content type of the file.
        if result_id is not None:
            res_dir = os.path.join(rs['resources_dir'], result_id)
        else:
            res_dir = os.path.join(rs['resources_dir'], rs['result_id'])
        res_file = os.path.join(res_dir, resource_id)
        if not os.path.isfile(res_file):
            raise err.UnknownResourceError(resource_id)
        #
        # Return handle for the resource file. Guess mime-type for the resource
        # file from the resource identifier
        mimetype, _ = mimetypes.guess_type(url=resource_id)
        return FileHandle(
            filepath=res_file,
            file_name=resource_id,
            mimetype=mimetype
        )

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
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given the
            schema default attribute is used
        include_all: bool, optional
            Include at most one entry per submission in the result if False

        Returns
        -------
        flowserv.model.ranking.ResultRanking

        Raises
        ------
        flowserv.core.error.UnknownBenchmarkError
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
        return ranking.query(
            con=self.con,
            benchmark_id=benchmark_id,
            schema=schema,
            filter_stmt='s.benchmark_id = ?',
            args=(benchmark_id,),
            order_by=order_by,
            include_all=include_all
        )

    def list_benchmarks(self):
        """Get a list of descriptors for all benchmarks in the repository.

        Returns
        -------
        list(flowserv.model.template.benchmark.BenchmarkHandle)
        """
        sql = 'SELECT benchmark_id, name, description, instructions '
        sql += 'FROM benchmark '
        result = list()
        for row in self.con.execute(sql).fetchall():
            # Return descriptors that will load associated workflow templates
            # on-demand
            result.append(
                BenchmarkHandle(
                    identifier=row['benchmark_id'],
                    name=row['name'],
                    description=row['description'],
                    instructions=row['instructions'],
                    repo=self
                )
            )
        return result

    def update_benchmark(
        self, benchmark_id, name=None, description=None, instructions=None
    ):
        """Update name, description, and instructions for a given benchmark.

        Raises an error if the given benchmark does not exist or if the name is
        not unique.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        name: string, optional
            Unique benchmark headline name
        description: string, optional
            Optional short description for display in benchmark listings
        instructions: string, optional
            Text containing detailed instructions for benchmark participants

        Returns
        -------
        flowserv.model.template.benchmark.BenchmarkHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownBenchmarkError
        """
        # Create the SQL update statement depending on the given arguments
        args = list()
        sql = 'UPDATE benchmark SET'
        if not name is None:
            # Ensure that the name is unique
            constraint_sql = 'SELECT name FROM benchmark '
            constraint_sql += 'WHERE name = ? AND benchmark_id <> ?'
            constraint.validate_name(
                name,
                con=self.con,
                sql=constraint_sql,
                args=(name, benchmark_id))
            args.append(name)
            sql += ' name = ?'
        if not description is None:
            if len(args) > 0:
                sql += ','
            args.append(description)
            sql += ' description = ?'
        if not instructions is None:
            if len(args) > 0:
                sql += ','
            args.append(instructions)
            sql += ' instructions = ?'
        # If none of the optional arguments was given we do not need to update
        # anything
        if len(args) > 0:
            args.append(benchmark_id)
            sql += ' WHERE benchmark_id = ?'
            self.con.execute(sql, args)
            self.con.commit()
        # Return the handle for the updated benchmark
        return self.get_benchmark(benchmark_id)
