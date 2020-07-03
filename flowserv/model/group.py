# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow group manager provides functionality to create and maintain
workflow groups. All information about groups is maintained in the underlying
database.
"""

import shutil

from flowserv.model.base import UploadFile, WorkflowGroup, WorkflowTemplate
from flowserv.model.user import UserManager

from flowserv.files import FileHandle

import flowserv.error as err
import flowserv.model.constraint as constraint
import flowserv.util as util


class WorkflowGroupManager(object):
    """Manager for workflow groups that associate a set of users with a set of
    workflow runs. The manager provides functionality to interact with the
    underlying database for creating and maintaining workflow groups.
    """
    def __init__(self, db, fs, users=None):
        """Initialize the connection to the underlying database and the file
        system helper to access group files.

        Parameters
        ----------
        db: flowserv.model.db.DB
            Database session.
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Helper to generate file system paths to group folders
        users: flowserv.model.user.UserManager
            Manager to access user objects.
        """
        self.db = db
        self.fs = fs
        self.users = users if users is not None else UserManager(db=db)

    def create_group(
        self, workflow_id, name, user_id, parameters, workflow_spec,
        members=None, commit_changes=True
    ):
        """Create a new group for a given workflow. Within each workflow,
        the names of groups are expected to be unique.

        The workflow group may define additional parameters for the template.
        The full (modifued or original) parameter list is stored with the group
        together with the workflow specification.

        A group may have a list of users that are members. Membership can be
        used to control which users are allowed to execute the associated
        workflow and to upload/view files. The user that creates the group,
        identified by user_id parameter, is always part of the initial list of
        group members.

        If a list of members is given it is ensured that each identifier in the
        list references an existing user.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        name: string
            Group name
        user_id: string
            Unique identifier of the user that created the group
        parameters: list(flowserv.model.parameter.base.TemplateParameter)
            List of workflow template parameter declarations that may be
            specific to the group
        workflow_spec: dict
            Workflow specification
        members: list(string), optional
            Optional list of user identifiers for other group members
        commit_changes: bool, optional
            Commit changes to database only if True

        Returns
        -------
        flowserv.model.base.WorkflowGroup

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownUserError
        """
        # Ensure that the given name is valid and unique for the workflow
        constraint.validate_name(name)
        group = self.db.session.query(WorkflowGroup)\
            .filter(WorkflowGroup.name == name)\
            .filter(WorkflowGroup.workflow_id == workflow_id)\
            .one_or_none()
        if group is not None:
            msg = "group '{}' exists".format(name)
            raise err.ConstraintViolationError(msg)
        # Create the group object
        identifier = util.get_unique_identifier()
        group = WorkflowGroup(
            group_id=identifier,
            name=name,
            workflow_id=workflow_id,
            owner_id=user_id,
            parameters=parameters,
            workflow_spec=workflow_spec
        )
        # Create a set of member identifier that contains the identifier of
        # the group owner. Ensure that all group members exist. This will also
        # ensure that the group owner exists.
        member_set = set([user_id]) if members is None else set(members)
        if user_id not in member_set:
            member_set.add(user_id)
        for member_id in member_set:
            group.members.append(self.users.get_user(member_id, active=True))
        # Create the group base directory.
        groupdir = self.fs.workflow_groupdir(workflow_id, identifier)
        util.create_dir(groupdir)
        # Enter group information into the database and commit all changes
        self.db.session.add(group)
        self.db.session.commit()
        return group

    def delete_file(self, group_id, file_id):
        """Delete uploaded group file with given identifier. Raises an error if
        the group or file does not exist.

        Parameters
        ----------
        group_id: string
            Unique group identifier
        file_id: string
            Unique file identifier

        Raises
        ------
        flowserv.error.UnknownWorkflowGroupError
        flowserv.error.UnknownFileError
        """
        # Get the group to ensure that it exists.
        group = self.get_group(group_id)
        # Get handle for the file.
        fh = None
        for i, file in enumerate(group.uploads):
            if file.file_id == file_id:
                del group.uploads[i]
                fh = FileHandle(
                    identifier=file_id,
                    name=file.name,
                    filename=self.fs.group_uploadfile(
                        workflow_id=group.workflow_id,
                        group_id=group.group_id,
                        file_id=file_id
                    )
                )
                break
        # No file with matching identifier was found.
        if fh is None:
            raise err.UnknownFileError(file_id)
        # Commit changes to database.
        self.db.session.commit()
        # If deleting the database record was successful delete the file on
        # disk
        fh.delete()

    def delete_group(self, group_id):
        """Delete the given workflow group and all associated resources.

        The changes to the underlying database are only commited if the
        commit_changes flag is True. Note that the deletion of files and
        directories cannot be rolled back.

        Parameters
        ----------
        group_id: string
            Unique group identifier
        commit_changes: bool, optional
            Commit changes to database only if True

        Raises
        ------
        flowserv.error.UnknownWorkflowGroupError
        """
        # Get group object from the database. If the result is None we
        # assume that the group does not exist and raise an error.
        group = self.get_group(group_id)
        groupdir = self.fs.workflow_groupdir(group.workflow_id, group_id)
        self.db.session.delete(group)
        self.db.session.commit()
        # Delete the base directory containing group files
        shutil.rmtree(groupdir)

    def get_file(self, group_id, file_id):
        """Get handle for an uploaded group file with the given identifier.
        Raises an error if the group or the file does not exists.

        Parameters
        ----------
        group_id: string
            Unique group identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        flowserv.files.FileHandle

        Raises
        ------
        flowserv.error.UnknownWorkflowGroupError
        flowserv.error.UnknownFileError
        """
        group = self.get_group(group_id)
        for file in group.uploads:
            if file.file_id == file_id:
                return FileHandle(
                    identifier=file_id,
                    name=file.name,
                    filename=self.fs.group_uploadfile(
                        workflow_id=group.workflow_id,
                        group_id=group.group_id,
                        file_id=file_id
                    )
                )
        # No file with matching identifier was found.
        raise err.UnknownFileError(file_id)

    def get_group(self, group_id):
        """Get handle for the workflow group with the given identifier.

        Parameters
        ----------
        group_id: string
            Unique group identifier

        Returns
        -------
        flowserv.model.base.WorkflowGroup

        Raises
        ------
        flowserv.error.UnknownWorkflowGroupError
        """
        group = self.db.session.query(WorkflowGroup)\
            .filter(WorkflowGroup.group_id == group_id)\
            .one_or_none()
        if group is None:
            raise err.UnknownWorkflowGroupError(group_id)
        return group

    def list_files(self, group_id):
        """Get list of file handles for all files that have been uploaded to
        a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique group identifier

        Returns
        -------
        list(flowserv.files.FileHandle)

        Raises
        ------
        flowserv.error.UnknownWorkflowGroupError
        """
        # Get the group object to ensure that the group exists.
        group = self.get_group(group_id)
        # Convert file objects to file handles.
        files = list()
        for file in group.uploads:
            fh = FileHandle(
                identifier=file.file_id,
                name=file.name,
                filename=self.fs.group_uploadfile(
                    workflow_id=group.workflow_id,
                    group_id=group.group_id,
                    file_id=file.file_id
                )
            )
            files.append(fh)
        return files

    def list_groups(self, workflow_id=None, user_id=None):
        """Get a listing of group descriptors. If the user identifier is given,
        only those groups are returned that the user is a member of. If the
        workflow identifier is given, only groups for the given workflow
        are included.

        Parameters
        ----------
        workflow_id: string, optional
            Unique workflow identifier
        user_id: string, optional
            Unique user identifier

        Returns
        -------
        list(flowserv.model.base.WorkflowGroup)
        """
        # Generate query depending on whether a user and workflow filter is
        # given or not.
        if workflow_id is None and user_id is None:
            # Return list of all groups.
            return self.db.session.query(WorkflowGroup).all()
        elif workflow_id is None:
            # Return all groups that a user is a member of.
            return self.users.get_user(user_id, active=True).groups
        elif user_id is None:
            # Return all groups for a workflow template.
            workflow = self.db.session.query(WorkflowTemplate)\
                .filter(WorkflowTemplate.workflow_id == workflow_id)\
                .one_or_none()
            if workflow is None:
                raise err.UnknownWorkflowError(workflow_id)
            return workflow.groups
        else:
            # Filter user groups for a particular workflow.
            user_groups = self.users.get_user(user_id, active=True).groups
            return [g for g in user_groups if g.workflow_id == workflow_id]

    def update_group(self, group_id, name=None, members=None):
        """Update the name and/or list of members for a workflow group.

        Parameters
        ----------
        group_id: string
            Unique group identifier
        name: string, optional
            Unique user identifier
        members: list(string), optional
            List of user identifier for group members

        Returns
        -------
        flowserv.model.base.WorkflowGroup

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownUserError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Get group handle. This will raise an error if the group is unknown.
        group = self.get_group(group_id)
        # If name and members are None we simply return the group handle.
        if name is None and members is None:
            return group
        if name is not None and name is not group.name:
            constraint.validate_name(name)
            group.name = name
        if members is not None:
            group.members = list()
            for user_id in members:
                group.members.append(self.users.get_user(user_id, active=True))
        self.db.session.commit()
        return group

    def upload_file(self, group_id, file, name, file_type=None):
        """Upload a new file for a workflow group. This will create a copy of
        the given file in the file store that is associated with the group. The
        file will be places in a unique folder inside the groups upload folder.

        Raises an error if the given file name is invalid.

        Parameters
        ----------
        group_id: string
            Unique group identifier
        file: file object (e.g., werkzeug.datastructures.FileStorage)
            File object (e.g., uploaded via HTTP request)
        name: string
            Name of the file
        file_type: string, optional
            Identifier for the file type (e.g., the file MimeType). This could
            also by the identifier of a content handler.

        Returns
        -------
        flowserv.files.FileHandle

        Raises
        ------
        flowserv.error.ConstraintViolationError
        flowserv.error.UnknownWorkflowGroupError
        """
        # Get the group object to ensure that the group exists.
        group = self.get_group(group_id)
        # Ensure that the given file name is valid
        constraint.validate_name(name)
        # Create a new unique identifier for the file and save the file object
        # to the new file path.
        file_id = util.get_unique_identifier()
        output_file = self.fs.group_uploadfile(
            workflow_id=group.workflow_id,
            group_id=group.group_id,
            file_id=file_id
        )
        file.save(output_file)
        # Insert information into database.
        fileobj = UploadFile(file_id=file_id, name=name, file_type=file_type)
        group.uploads.append(fileobj)
        self.db.session.commit()
        # Return handle for uploaded file
        return FileHandle(
            identifier=file_id,
            filename=output_file,
            name=name
        )
