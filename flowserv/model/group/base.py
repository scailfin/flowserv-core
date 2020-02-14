# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Workflow access groups are containers for sets of users and sets of workflow
runs. Groups are primarily intended for benchmarks. For benchmarks, each group
should be viewed as an entry (or submission) to the benchmark.

Uploaded files are maintained with each group and accessible by group members
only. Group membership is also used for access control when starting a new
workflow run.
"""

from flowserv.core.files import FileHandle

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.model.constraint as constraint


class WorkflowGroupDescriptor(object):
    """The descriptor for a workflow group contains only the group identifier,
    the group name, and the identifier of the associated workflow.
    """
    def __init__(self, identifier, name, workflow_id):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique group identifier
        name: string
            Unique group name
        workflow_id: string
            Unique workflow identifier
        """
        self.identifier = identifier
        self.name = name
        self.workflow_id = workflow_id


class WorkflowGroupHandle(WorkflowGroupDescriptor):
    """A workflow group is a container for sets of users and sets of workflow
    runs. Each group is associated with a workflow template.  When the group is
    created, variations to the original workflow may be made to the workflow
    specification and the template parameter declarations. The group handle
    maintains a modified copy of the respective parts of the workflow template.

    Each group has a name that uniquely identifies it among all groups for a
    workflow template. The group is created by a user (the owner) who can
    invite other users as group members.

    With each group a set of uploaded files is maintained. Access to these
    files is restricted to members in the workflow group.
    """
    def __init__(
        self, con, identifier, name, workflow_id, owner_id, parameters,
        workflow_spec, fs, members=None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        identifier: string
            Unique group identifier
        name: string
            Unique group name
        workflow_id: string
            Unique workflow identifier
        owner_id: string
            Unique identifier for the user that created the group
        parameters: dict(string:flowserv.model.parameter.base.TemplateParameter)
            Workflow template parameter declarations
        workflow_spec: dict
            Workflow specification
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Helper to generate file system paths to group folders
        members: list(flowserv.model.user.base.UserHandle)
            List of handles for group members (includes the handle for the
            group owner if the owner is still a member)
        """
        super(WorkflowGroupHandle, self).__init__(
            identifier=identifier,
            name=name,
            workflow_id=workflow_id
        )
        self.con = con
        self.owner_id = owner_id
        self.parameters = parameters
        self.workflow_spec = workflow_spec
        self.fs = fs
        self.members = members
        # Ensure that the folder for uploaded files exists
        util.create_dir(self.fs.group_uploaddir(workflow_id, identifier))

    def delete_file(self, file_id, commit_changes=True):
        """Delete file with given identifier. Raises an error if the file does
        not exist.

        Parameters
        ----------
        file_id: string
            Unique file identifier
        commit_changes: bool, optional
            Commit changes to database only if True

        Raises
        ------
        flowserv.core.error.UnknownFileError
        """
        # Get the file handle which contains the path to the file on disk.
        # This will raise an error if the file does not exist
        fh = self.get_file(file_id)
        # Delete the record from the database first.
        sql = 'DELETE FROM group_upload_file WHERE file_id = ?'
        self.con.execute(sql, (file_id,))
        if commit_changes:
            self.con.commit()
        # If deleting the database record was successful delete the file on
        # disk
        fh.delete()

    def get_file(self, file_id):
        """Get handle for file with given identifier. Raises an error if no
        file with given identifier exists.

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
        # Get name of the file from the underlying database. Raise error if the
        # result is empty.
        sql = (
            'SELECT name FROM group_upload_file '
            'WHERE group_id = ? AND file_id = ?'
        )
        row = self.con.execute(sql, (self.identifier, file_id)).fetchone()
        if row is None:
            raise err.UnknownFileError(file_id)
        return FileHandle(
            identifier=file_id,
            name=row['name'],
            filename=self.fs.group_uploadfile(
                workflow_id=self.workflow_id,
                group_id=self.identifier,
                file_id=file_id
            )
        )

    def list_files(self):
        """Get list of file handles for all files that have been uploaded to
        the workflow group.

        Returns
        -------
        list(flowserv.core.files.FileHandle)
        """
        # Get list of uploaded files from the underlying database
        sql = 'SELECT file_id, name FROM group_upload_file WHERE group_id = ?'
        result = list()
        for row in self.con.execute(sql, (self.identifier,)).fetchall():
            file_id = row['file_id']
            fh = FileHandle(
                identifier=file_id,
                name=row['name'],
                filename=self.fs.group_uploadfile(
                    workflow_id=self.workflow_id,
                    group_id=self.identifier,
                    file_id=file_id
                )
            )
            result.append(fh)
        return result

    def upload_file(self, file, name, file_type=None, commit_changes=True):
        """Upload a new file for the workflow group. This will create a copy of
        the given file in the file store that is associated with the group. The
        file will be places in a unique folder inside the groups upload folder.

        Raises an error if the given file name is invalid.

        Parameters
        ----------
        file: file object (e.g., werkzeug.datastructures.FileStorage)
            File object (e.g., uploaded via HTTP request)
        name: string
            Name of the file
        file_type: string, optional
            Identifier for the file type (e.g., the file MimeType). This could
            also by the identifier of a content handler.
        commit_changes: bool, optional
            Commit changes to database only if True

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        """
        # Ensure that the given file name is valid
        constraint.validate_name(name)
        # Create a new unique identifier for the file and save the file object
        # to the new file path.
        identifier = util.get_unique_identifier()
        output_file = self.fs.group_uploadfile(
            workflow_id=self.workflow_id,
            group_id=self.identifier,
            file_id=identifier
        )
        file.save(output_file)
        # Insert information into database
        sql = (
            'INSERT INTO group_upload_file'
            '(group_id, file_id, name, file_type) '
            'VALUES(?, ?, ?, ?)'
        )
        self.con.execute(sql, (self.identifier, identifier, name, file_type))
        if commit_changes:
            self.con.commit()
        # Return handle for uploaded file
        return FileHandle(
            identifier=identifier,
            filename=output_file,
            name=name
        )
