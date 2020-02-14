# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Descriptor and handle for workflow definitions that are maintained by the
API in a workflow repository.
"""

from flowserv.model.template.base import WorkflowTemplate


class WorkflowDescriptor(object):
    """the workflow descriptor maintains basic information about a workflows in
    a repository. The descriptors are primarily intended for workflow listings
    that only display the basic workflow information. All additional objects
    that are associated with a workflow are contained in the workflow handle
    that extends the workflow descriptor.
    """
    def __init__(
        self, identifier, name=None, description=None, instructions=None
    ):
        """Initialize the descriptor properties. If no name is given the
        identifier is used as a name.

        Parameters
        ----------
        identifier: string
            Unique workflow identifier
        template: flowserv.model.template.base.WorkflowTemplate
            Template for the associated workflow
        name: string, optional
            Descriptive workflow name
        description: string, optional
            Optional short description for display in workflow listings
        instructions: string, optional
            Text containing detailed instructions for running the workflow
        """
        self.identifier = identifier
        self.name = name if name is not None else identifier
        self.description = description
        self.instructions = instructions

    def get_description(self):
        """Get value of description property. If the value of the property is
        None an empty string is returned instead.

        Returns
        -------
        string
        """
        return self.description if self.description is not None else ''

    def get_instructions(self):
        """Get value of instructions property. If the value of the property is
        None an empty string is returned instead.

        Returns
        -------
        string
        """
        return self.instructions if self.instructions is not None else ''

    def has_description(self):
        """Shortcut to test of the description attribute is set.

        Returns
        -------
        bool
        """
        return self.description is not None

    def has_instructions(self):
        """Test if the instructions for the workflow are set.

        Returns
        -------
        bool
        """
        return self.instructions is not None


class WorkflowHandle(WorkflowDescriptor):
    """The workflow handle extends the workflow descriptor with references to
    the workflow template and the latest post-processing workflow run handle if
    a post-processing workflow is defined in the template.
    """
    def __init__(
        self, con, identifier, template, name=None, description=None,
        instructions=None, postproc_run=None
    ):
        """Initialize the handle properties. If no name is given the
        identifier is used as a name.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        identifier: string
            Unique workflow identifier
        template: flowserv.model.template.base.WorkflowTemplate
            Template for the associated workflow
        name: string, optional
            Descriptive workflow name
        description: string, optional
            Optional short description for display in workflow listings
        instructions: string, optional
            Text containing detailed instructions for running the workflow
        postproc_run: flowserv.model.run.base.RunHandle, optional
            Optional handle for workflow run that computed post-processing
            results
        """
        super(WorkflowHandle, self).__init__(
            identifier=identifier,
            name=name,
            description=description,
            instructions=instructions
        )
        self.con = con
        self.template = template
        self.postproc_run = postproc_run

    def get_postproc_key(self):
        """Get sorted list of identifier for group run results that were used
        to generate the current set of post-processing results.

        Returns
        -------
        list(string)
        """
        result = list()
        # If the post-processing run is not set, the result is empty
        if self.postproc_run is not None:
            sql = (
                'SELECT group_run_id '
                'FROM workflow_postproc p, workflow_template w '
                'WHERE w.postproc_id = p.postproc_id AND w.workflow_id = ?'
            )
            for row in self.con.execute(sql, (self.identifier,)).fetchall():
                result.append(row['group_run_id'])
            # Sort keys in the for easy comparison.
            return sorted(result)
        return result

    def get_schema(self):
        """Short-cut to get access to the workflow schema in the template.

        Returns
        -------
        flowserv.model.template.schema.ResultSchema
        """
        return self.template.get_schema()

    def get_template(self, workflow_spec=None, parameters=None):
        """Get associated workflow template. The template is loaded on-demand
        if necessary. If either of the optional parameters are given, a
        modified copy of the template is returned.

        Returns
        -------
        flowserv.model.template.base.WorkflowTemplate
        """
        # If any of the optional parameters are given return a modified copy of
        # the workflow template.
        if workflow_spec and parameters:
            return WorkflowTemplate(
                workflow_spec=workflow_spec,
                parameters=parameters,
                sourcedir=self.template.sourcedir,
                result_schema=self.template.result_schema,
                modules=self.template.modules,
                postproc_spec=self.template.postproc_spec
            )
        elif workflow_spec:
            return WorkflowTemplate(
                workflow_spec=workflow_spec,
                parameters=self.template.parameters,
                sourcedir=self.template.sourcedir,
                result_schema=self.template.result_schema,
                modules=self.template.modules,
                postproc_spec=self.template.postproc_spec
            )
        elif parameters:
            return WorkflowTemplate(
                workflow_spec=self.template.workflow_spec,
                parameters=parameters,
                sourcedir=self.template.sourcedir,
                result_schema=self.template.result_schema,
                modules=self.template.modules,
                postproc_spec=self.template.postproc_spec
            )
        return self.template

    def update_postproc(self, run_id, runs, commit_changes=True):
        """Update the current post-processing run for the workflow.

        Parameters
        ----------
        run_id: string
            Unique identifier of the post-porcessing run
        runs: list(string)
            Identifier of runs in the post-porcessing key
        commit_changes: bool, optional
            Commit changes to database only if True
        """
        # Insert run keys into workflow_postproc
        sql = (
            'INSERT INTO workflow_postproc('
            'postproc_id, workflow_id, group_run_id) '
            'VALUES(?, ?, ?)'
        )
        for group_run_id in runs:
            self.con.execute(sql, (run_id, self.identifier, group_run_id))
        # Update the workflow template
        sql = (
            'UPDATE workflow_template '
            'SET postproc_id = ? '
            'WHERE workflow_id = ?'
        )
        self.con.execute(sql, (run_id, self.identifier))
        # Commit changes if requested
        if commit_changes:
            self.con.commit()
