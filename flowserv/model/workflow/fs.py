# This file is part of Flowserv.
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper class that defines the folder structure of the file system for
workflows and their associated resources.

The folder structure is currently as follows:

/workflows                   : Base directory
    {workflow_id}            : Folder for individual workflow
        resources            : Folder for results of workflow post-processing
            {postproc_id}    : Result files for individual post-processing runs
        groups               : Folder for workflow groups
            {group_id}       : Folder for individual group
                files        : Uploaded files for workflow group
                    {file_id}: Folder for uploaded file
                runs         : Workflow runs that are associated with the group
                    {run_id} : Individual run folder
"""

import os

import flowserv.core.util as util


class WorkflowFileSystem(object):
    """Generator for file system folders that are associated with workflow
    templates.
    """
    def __init__(self, basedir):
        """Initialize the base diretory for all workflow resources.

        Parameters
        ----------
        basedir: string
            Path to directory on the file system
        """
        # Ensure that the base directory exists
        self.basedir = util.create_dir(basedir, abs=True)

    def group_file(self, workflow_id, group_id, file_id):
        """Get path for a file that was uploaded to a workflow group.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        string
        """
        uploaddir = self.group_uploaddir(workflow_id, group_id)
        return os.path.join(uploaddir, file_id)

    def group_uploaddir(self, workflow_id, group_id):
        """Get base directory for files that are uploaded to a workflow group.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        groupdir = self.workflow_groupdir(workflow_id, group_id)
        return os.path.join(groupdir, 'files')

    def run_basedir(self, workflow_id, group_id, run_id):
        """Get path to the base directory for all files that are maintained for
        workflow runs.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        group_id: string
            Unique workflow group identifier
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        groupdir = self.workflow_groupdir(workflow_id, group_id)
        return os.path.join(groupdir, 'runs', run_id)

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

    def workflow_groupdir(self, workflow_id, group_id):
        """Get base directory containing files that are associated with a
        workflow group.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        workflowdir = self.workflow_basedir(workflow_id)
        return os.path.join(workflowdir, 'groups', group_id)

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