# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow repository maintains information about registered workflow
templates. For each template additional basic information is stored in the
underlying database.
"""

import errno
import git
import json
import os
import shutil

from flowserv.model.parameter.base import ParameterGroup
from flowserv.model.run.manager import RunManager
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.template.schema import ResultSchema
from flowserv.model.workflow.base import WorkflowDescriptor, WorkflowHandle

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.constraint as constraint
import flowserv.model.parameter.base as pb


""" "Default values for the max. attempts parameter and the ID generator
function.
"""
DEFAULT_ATTEMPTS = 100
DEFAULT_IDFUNC = util.get_short_identifier

"""Names for files that are used to identify template specification."""
DEFAULT_SPECNAMES = ['benchmark', 'workflow', 'template']
DEFAULT_SPECSUFFIXES = ['.yml', '.yaml', '.json']


class WorkflowRepository(object):
    """The workflow repository maintains information that is associated with
    workflow templates in a given repository.
    """
    def __init__(self, con, fs, idfunc=None, attempts=None, tmpl_names=None):
        """Initialize the database connection, and the generator for workflow
        related file names and directory paths. The optional parameters are
        used to configure the identifier function that is used to generate
        unique workflow identifier as well as the list of default file names
        for template specification files.

        By default, short identifiers are used.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Generattor for file names and directory paths
        idfunc: func, optional
            Function to generate template folder identifier
        attempts: int, optional
            Maximum number of attempts to create a unique folder for a new
            workflow template
        tmpl_names: list(string), optional
            List of default names for template specification files
        """
        self.con = con
        self.fs = fs
        # Initialize the identifier function and the max. number of attempts
        # that are made to generate a unique identifier.
        self.idfunc = idfunc if idfunc is not None else DEFAULT_IDFUNC
        self.attempts = attempts if attempts is not None else DEFAULT_ATTEMPTS
        # Initialize the list of default template specification file names
        if tmpl_names is not None:
            self.default_filenames = tmpl_names
        else:
            self.default_filenames = list()
            for name in DEFAULT_SPECNAMES:
                for suffix in DEFAULT_SPECSUFFIXES:
                    self.default_filenames.append(name + suffix)

    def create_workflow(
        self, name, description=None, instructions=None, sourcedir=None,
        repourl=None, specfile=None, commit_changes=True
    ):
        """Add new workflow to the repository. The associated workflow template
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
        # Exactly one of sourcedir and repourl has to be not None. If both
        # are None (or not None) a ValueError is raised.
        if sourcedir is None and repourl is None:
            raise ValueError('no source folder or repository url given')
        elif sourcedir is not None and repourl is not None:
            raise ValueError('source folder and repository url given')
        # Ensure that the workflow name is not empty, not longer than 512
        # character and unique.
        sql = 'SELECT name FROM workflow_template WHERE name = ?'
        constraint.validate_name(name, con=self.con, sql=sql)
        # Create identifier and folder for the workflow
        workflow_id, workflowdir = self.create_folder(self.fs.workflow_basedir)
        # Copy either the given workflow directory into the folder for static
        # files or clone the Git repository into the folder.
        staticdir = self.fs.workflow_staticdir(workflow_id)
        try:
            if sourcedir is not None:
                shutil.copytree(src=sourcedir, dst=staticdir)
            else:
                git.Repo.clone_from(repourl, staticdir)
        except (IOError, OSError, git.exc.GitCommandError) as ex:
            # Make sure to cleanup by removing the created template folder
            shutil.rmtree(workflowdir)
            raise ex
        # Find template specification file in the template workflow folder.
        # If the file is not found the workflow directory is removed and an
        # error is raised.
        template = None
        if specfile is not None:
            candidates = [specfile]
        else:
            candidates = list()
            for filename in self.default_filenames:
                candidates.append(os.path.join(staticdir, filename))
        for filename in candidates:
            if os.path.isfile(filename):
                # Read template from file. If no error occurs the folder
                # contains a valid template.
                template = WorkflowTemplate.from_dict(
                    doc=util.read_object(filename),
                    sourcedir=staticdir,
                    validate=True
                )
                break
        # No template file found. Cleanup and raise error.
        if template is None:
            shutil.rmtree(workflowdir)
            raise err.InvalidTemplateError('no template file found')
        # Insert workflow into database and return descriptor. Database changes
        # are only commited if the respective flag is True.
        sql = (
            'INSERT INTO workflow_template(workflow_id, name, description, '
            'instructions, workflow_spec, parameters, modules, postproc_spec, '
            'result_schema) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
        )
        # Serialize values for optional template elements
        parameters = [p.to_dict() for p in template.parameters.values()]
        parameters = json.dumps(parameters) if len(parameters) > 0 else None
        postproc = template.postproc_spec
        postproc = json.dumps(postproc) if postproc is not None else None
        modules = template.modules
        if modules is not None:
            modules = json.dumps([m.to_dict() for m in modules])
        schema = template.result_schema
        schema = json.dumps(schema.to_dict()) if schema is not None else None
        args = (
            workflow_id,
            name,
            description,
            instructions,
            json.dumps(template.workflow_spec),
            parameters,
            modules,
            postproc,
            schema
        )
        self.con.execute(sql, args)
        if commit_changes:
            self.con.commit()
        return WorkflowHandle(
            con=self.con,
            identifier=workflow_id,
            name=name,
            description=description,
            instructions=instructions,
            template=template
        )

    def create_folder(self, dirfunc):
        """Create a new unique folder in a base directory using the internal
        identifier function. The path to the created folder is generated using
        the given directory function that takes a unique identifier as the only
        argument.

        Returns a tuple containing the identifier and the directory. Raises
        an error if the maximum number of attempts to create the unique folder
        was reached.

        Parameters
        ----------
        dirfunc: func
            Function to generate the path for the created folder

        Returns
        -------
        (id::string, subfolder::string)

        Raises
        ------
        ValueError
        """
        identifier = None
        attempt = 0
        while identifier is None:
            # Create a new identifier
            identifier = self.idfunc()
            # Try to generate the subfolder. If the folder exists, set
            # identifier to None to signal failure.
            subfolder = dirfunc(identifier)
            if os.path.isdir(subfolder):
                identifier = None
            else:
                try:
                    os.makedirs(subfolder)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                    else:
                        # Directory must have been created concurrently
                        identifier = None
            if identifier is None:
                # Increase number of failed attempts. If the maximum number of
                # attempts is reached raise an errir
                attempt += 1
                if attempt > self.attempts:
                    raise RuntimeError('could not create unique folder')
        return identifier, subfolder

    def delete_workflow(self, workflow_id, commit_changes=True):
        """Delete the workflow with the given identifier.

        The changes to the underlying database are only commited if the
        commit_changes flag is True. Note that the deletion of files and
        directories cannot be rolled back.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        commit_changes: bool, optional
            Commit changes to database only if True

        Raises
        ------
        flowserv.core.error.UnknownWorkflowError
        """
        # Get the base directory for the workflow. If the directory does not
        # exist we assume that the workflow is unknown and raise an error.
        workflowdir = self.fs.workflow_basedir(workflow_id)
        if not os.path.isdir(workflowdir):
            raise err.UnknownWorkflowError(workflow_id)
        # Create list of SQL statements to delete all records that are
        # associated with the workflow
        stmts = list()
        # -- Workflow Runs ----------------------------------------------------
        stmts.append(
            'DELETE FROM run_result_file WHERE run_id IN ('
            '   SELECT r.run_id FROM workflow_run r WHERE r.workflow_id = ?)'
        )
        stmts.append(
            'DELETE FROM run_error_log WHERE run_id IN ('
            '   SELECT r.run_id FROM workflow_run r WHERE r.workflow_id = ?)'
        )
        stmts.append('DELETE FROM workflow_run WHERE workflow_id = ?')
        # -- Workflow Group ---------------------------------------------------
        stmts.append(
            'DELETE FROM group_member WHERE group_id IN ('
            '   SELECT g.group_id FROM workflow_group g '
            '   WHERE g.workflow_id = ?)'
        )
        stmts.append(
            'DELETE FROM group_upload_file WHERE group_id IN ('
            '   SELECT g.group_id FROM workflow_group g '
            '   WHERE g.workflow_id = ?)'
        )
        stmts.append('DELETE FROM workflow_group WHERE workflow_id = ?')
        # -- Workflow Template ------------------------------------------------
        stmts.append('DELETE FROM workflow_postproc WHERE workflow_id = ?')
        stmts.append('DELETE FROM workflow_template WHERE workflow_id = ?')
        for sql in stmts:
            self.con.execute(sql, (workflow_id,))
        if commit_changes:
            self.con.commit()
        # Delete all files that are associated with the workflow
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
            'SELECT workflow_id, name, description, instructions, postproc_id,'
            'workflow_spec, parameters, modules, postproc_spec, result_schema '
            'FROM workflow_template '
            'WHERE workflow_id = ?'
        )
        row = self.con.execute(sql, (workflow_id,)).fetchone()
        if row is None:
            raise err.UnknownWorkflowError(workflow_id)
        name = row['name']
        description = row['description']
        instructions = row['instructions']
        postproc_id = row['postproc_id']
        # Get handles for post-processing workflow run
        postproc_run = None
        if postproc_id is not None:
            run_manager = RunManager(con=self.con, fs=self.fs)
            postproc_run = run_manager.get_run(run_id=postproc_id)
        # Create workflow template
        parameters = None
        if row['parameters'] is not None:
            parameters = pb.create_parameter_index(
                json.loads(row['parameters']),
                validate=False
            )
        modules = None
        if row['modules'] is not None:
            modules = list()
            for m in json.loads(row['modules']):
                modules.append(ParameterGroup.from_dict(m))
        postproc_spec = None
        if row['postproc_spec'] is not None:
            postproc_spec = json.loads(row['postproc_spec'])
        result_schema = None
        if row['result_schema'] is not None:
            doc = json.loads(row['result_schema'])
            result_schema = ResultSchema.from_dict(doc)
        template = WorkflowTemplate(
            workflow_spec=json.loads(row['workflow_spec']),
            sourcedir=self.fs.workflow_staticdir(workflow_id),
            parameters=parameters,
            modules=modules,
            postproc_spec=postproc_spec,
            result_schema=result_schema
        )
        # Return workflow handle
        return WorkflowHandle(
            con=self.con,
            identifier=workflow_id,
            name=name,
            description=description,
            instructions=instructions,
            template=template,
            postproc_run=postproc_run
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
