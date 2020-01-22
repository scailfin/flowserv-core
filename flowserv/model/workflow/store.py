# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow repository maintains information about registered workflow
templates. For each template additional basic information is stored in the
underlying database.
"""

import os
import shutil

from flowserv.model.workflow.base import WorkflowDescriptor, WorkflowHandle
from flowserv.model.workflow.resource import FSObject

import flowserv.core.error as err
import flowserv.model.constraint as constraint
import flowserv.core.util as util


class WorkflowRepository(object):
    """The workflow repository maintains information that is associated with
    workflow templates in a given repository.
    """
    def __init__(self, con, template_repo, basedir):
        """Initialize the database connection, the template repository, and the
        base directory for maintaining post-processing results.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        template_store: flowserv.model.template.store.base.TemplateRepository, optional
            Repository for workflow templates
        basedir: string
            Path to the base directory that contains post-processing results
            for workflows
        """
        self.con = con
        self.template_repo = template_repo
        # Create the resource directory if it does not exist
        self.basedir = util.create_dir(basedir, abs=True)

    def add_workflow(
        self, name, description=None, instructions=None, sourcedir=None,
        repourl=None, specfile=None, commit_changes=True
    ):
        """Add a workflow to the repository. The associated workflow template
        is created in the template repository from either the given source
        directory or a Git repository. The template repository will raise an
        error if neither or both arguments are given.

        Each workflow has a name and an optional description and set of
        instructions.

        Raises an error if the given workflow name is not unique.

        Parameters
        ----------
        name: string
            Unique workflow name
        description: string, optional
            Optional short description for display in workflow listings
        instructions: string, optional
            Text containing detailed instructions for running the workflow
        sourcedir: string, optional
            Directory containing the workflow static files and the workflow
            template specification.
        repourl: string, optional
            Git repository that contains the the workflow files
        specfile: string, optional
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)
        commit_changes: bool, optional
            Commit changes to database only if True

        Returns
        -------
        flowserv.model.workflow.base.WorkflowHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.InvalidTemplateError
        ValueError
        """
        # Ensure that the workflow name is not empty, not longer than 512
        # character and unique.
        sql = 'SELECT name FROM workflow_template WHERE name = ?'
        constraint.validate_name(name, con=self.con, sql=sql)
        # Create the workflow template in the associated template repository
        workflow_id, template = self.template_repo.add_template(
            sourcedir=sourcedir,
            repourl=repourl,
            specfile=specfile
        )
        # Create the base directory for workflow runs and post-processing
        # results
        util.create_dir(self.workflow_basedir(workflow_id))
        # Insert workflow into database and return descriptor. Database changes
        # are only commited if the respective flag is True.
        sql = (
            'INSERT INTO workflow_template'
            '(workflow_id, name, description, instructions) '
            'VALUES (?, ?, ?, ?)'
        )
        self.con.execute(sql, (workflow_id, name, description, instructions))
        if commit_changes:
            self.con.commit()
        return WorkflowHandle(
            identifier=workflow_id,
            name=name,
            description=description,
            instructions=instructions,
            template=template
        )

    def delete_workflow(self, workflow_id, commit_changes=True):
        """Delete the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        commit_changes: bool, optional
            Commit changes to database only if True
        """
        # Delete the workflow template.
        self.template_repo.delete_template(workflow_id)
        # Create list of SQL statements to delete all records that are
        # associated with the workflow
        stmts = list()
        # run_result_file
        stmts.append(
            'DELETE FROM run_result_file WHERE run_id IN ('
            'SELECT r.run_id FROM workflow_run r, workflow_group g '
            'WHERE r.group_id = g.group_id AND g.workflow_id = ?)'
        )
        # run_error_log
        stmts.append(
            'DELETE FROM run_error_log WHERE run_id IN ('
            'SELECT r.run_id FROM workflow_run r, workflow_group g '
            'WHERE r.group_id = g.group_id AND g.workflow_id = ?)'
        )
        # workflow_run
        stmts.append(
            'DELETE FROM workflow_run WHERE group_id IN ('
            'SELECT g.group_id FROM workflow_group g '
            'WHERE g.workflow_id = ?)'
        )
        # group_member
        stmts.append(
            'DELETE FROM group_member WHERE group_id IN ('
            'SELECT g.group_id FROM workflow_group g '
            'WHERE g.workflow_id = ?)'
        )
        # group_upload_file
        stmts.append(
            'DELETE FROM group_upload_file WHERE group_id IN ('
            'SELECT g.group_id FROM workflow_group g '
            'WHERE g.workflow_id = ?)'
        )
        # workflow_group
        stmts.append('DELETE FROM workflow_group WHERE workflow_id = ?')
        # postproc_resource
        stmts.append(
            'DELETE FROM postproc_resource WHERE postproc_id IN ('
            'SELECT p.postproc_id FROM workflow_postproc p '
            'WHERE p.workflow_id = ?)'
        )
        # workflow_postproc
        stmts.append('DELETE FROM workflow_postproc WHERE workflow_id = ?')
        # workflow_template
        stmts.append('DELETE FROM workflow_template WHERE workflow_id = ?')
        for sql in stmts:
            self.con.execute(sql, (workflow_id,))
        if commit_changes:
            self.con.commit()
        # Delete all files that are associated with the workflow
        workflowdir = self.workflow_basedir(workflow_id)
        if os.path.isdir(workflowdir):
            shutil.rmtree(workflowdir)

    def get_workflow(self, workflow_id):
        """Get handle for the workflow with the given identifier. Raises
        an error if no workflow with the identifier exists.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        flowserv.model.workflow.base.WorkflowHandle

        Raises
        ------
        flowserv.core.error.UnknownWorkflowError
        """
        # Get workflow information from database. If the result is empty an
        # error is raised
        sql = (
            'SELECT workflow_id, name, description, instructions, postproc_id '
            'FROM workflow_template '
            'WHERE workflow_id = ?'
        )
        rs = self.con.execute(sql, (workflow_id,)).fetchone()
        if rs is None:
            raise err.UnknownWorkflowError(workflow_id)
        name = rs['name']
        description = rs['description']
        instructions = rs['instructions']
        postproc_id = rs['postproc_id']
        # Get resource handles for current post-processing resources
        resources = list()
        if postproc_id is not None:
            resourcedir = self.workflow_resourcedir(workflow_id, postproc_id)
            sql = (
                'SELECT resource_id, resource_name '
                'FROM postproc_resource '
                'WHERE postproc_id = ?'
            )
            for row in self.con.execute(sql, (postproc_id,)).fetchall():
                resource_name = row['resource_name']
                fsobj = FSObject(
                    identifier=row['resource_id'],
                    name=resource_name,
                    filename=os.path.join(resourcedir, resource_name)
                )
                resources.append(fsobj)
        # Return workflow handle
        return WorkflowHandle(
            identifier=workflow_id,
            name=name,
            description=description,
            instructions=instructions,
            template=self.template_repo.get_template(workflow_id),
            resources=resources
        )

    def list_workflows(self):
        """Get a list of descriptors for all workflows in the repository.

        Returns
        -------
        list(flowserv.model.workflow.base.WorkflowDescriptor)
        """
        sql = 'SELECT workflow_id, name, description, instructions '
        sql += 'FROM workflow_template '
        result = list()
        for row in self.con.execute(sql).fetchall():
            result.append(
                WorkflowDescriptor(
                    identifier=row['workflow_id'],
                    name=row['name'],
                    description=row['description'],
                    instructions=row['instructions']
                )
            )
        return result

    def update_workflow(
        self, workflow_id, name=None, description=None, instructions=None,
        commit_changes=True
    ):
        """Update name, description, and instructions for a given workflow.

        Raises an error if the given workflow does not exist or if the name is
        not unique.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        name: string, optional
            Unique workflow name
        description: string, optional
            Optional short description for display in workflow listings
        instructions: string, optional
            Text containing detailed instructions for workflow execution
        commit_changes: bool, optional
            Commit changes to database only if True

        Returns
        -------
        flowserv.model.workflow.base.WorkflowHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownWorkflowError
        """
        # Create the SQL update statement depending on the given arguments
        args = list()
        sql = 'UPDATE workflow_template SET'
        if name is not None:
            # Ensure that the name is unique
            constraint_sql = 'SELECT name FROM workflow_template '
            constraint_sql += 'WHERE name = ? AND workflow_id <> ?'
            constraint.validate_name(
                name,
                con=self.con,
                sql=constraint_sql,
                args=(name, workflow_id))
            args.append(name)
            sql += ' name = ?'
        if description is not None:
            if len(args) > 0:
                sql += ','
            args.append(description)
            sql += ' description = ?'
        if instructions is not None:
            if len(args) > 0:
                sql += ','
            args.append(instructions)
            sql += ' instructions = ?'
        # If none of the optional arguments was given we do not need to update
        # anything
        if len(args) > 0:
            args.append(workflow_id)
            sql += ' WHERE workflow_id = ?'
            self.con.execute(sql, args)
            if commit_changes:
                self.con.commit()
        # Return the handle for the updated workflow
        return self.get_workflow(workflow_id)

    def workflow_basedir(self, workflow_id):
        """Get base directory containing associated files for the workflow with
        the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        string
        """
        return os.path.join(self.basedir, workflow_id)

    def workflow_resourcedir(self, workflow_id, postproc_id):
        """Get base directory containing results for a post-processing run for
        the given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        postproc_id: string
            Unique post-processing run identifier

        Returns
        -------
        string
        """
        workflowdir = self.workflow_basedir(workflow_id)
        return os.path.join(workflowdir, 'resources', postproc_id)
