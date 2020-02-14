# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow group manager provides functionality to create and maintain
workflow groups. All information about groups is maintained in the underlying
database.
"""

import json
import shutil

from flowserv.model.group.base import WorkflowGroupDescriptor, WorkflowGroupHandle
from flowserv.model.user.base import UserHandle

import flowserv.core.error as err
import flowserv.model.constraint as constraint
import flowserv.model.parameter.base as pb
import flowserv.core.util as util


class WorkflowGroupManager(object):
    """Manager for workflow groups that associate a set of users with a set of
    workflow runs. The manager provides functionality to interact with the
    underlying database for creating and maintaining workflow groups.
    """
    def __init__(self, con, fs):
        """Initialize the connection to the underlying database and the file
        system helper to access group files.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        fs: flowserv.model.workflow.fs.WorkflowFileSystem
            Helper to generate file system paths to group folders
        """
        self.con = con
        self.fs = fs

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
        flowserv.model.group.base.WorkflowGroupHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownUserError
        """
        # Ensure that the given name is valid and unique for the workflow
        sql = (
            'SELECT name FROM workflow_group '
            'WHERE workflow_id = ? AND name = ?'
        )
        args = (workflow_id, name)
        constraint.validate_name(name, con=self.con, sql=sql, args=args)
        # Create a set of member identifier that contains the identifier of
        # the group owner
        if members is None:
            member_set = set([user_id])
        else:
            member_set = set(members)
            if user_id not in member_set:
                member_set.add(user_id)
        # Ensure that all group members exist. Create list of user handles for
        # group members
        users = list()
        sql = 'SELECT name FROM api_user WHERE user_id = ?'
        for member_id in member_set:
            row = self.con.execute(sql, (member_id,)).fetchone()
            if row is None:
                raise err.UnknownUserError(member_id)
            else:
                user = UserHandle(identifier=member_id, name=row['name'])
                users.append(user)
        # Create a unique identifier for the group and the group base directory
        identifier = util.get_unique_identifier()
        groupdir = self.fs.workflow_groupdir(workflow_id, identifier)
        util.create_dir(groupdir)
        # Enter group information into the database and commit all changes
        sql = (
            'INSERT INTO workflow_group('
            'group_id, workflow_id, name, owner_id, parameters, workflow_spec'
            ') VALUES(?, ?, ?, ?, ?, ?)'
        )
        values = (
            identifier,
            workflow_id,
            name,
            user_id,
            json.dumps([p.to_dict() for p in parameters.values()]),
            json.dumps(workflow_spec)
        )
        self.con.execute(sql, values)
        sql = (
            'INSERT INTO group_member(group_id, user_id) '
            'VALUES(?, ?)'
        )
        for member_id in member_set:
            self.con.execute(sql, (identifier, member_id))
        if commit_changes:
            self.con.commit()
        # Return the created group handle
        return WorkflowGroupHandle(
            con=self.con,
            identifier=identifier,
            name=name,
            workflow_id=workflow_id,
            owner_id=user_id,
            parameters=parameters,
            workflow_spec=workflow_spec,
            fs=self.fs,
            members=users
        )

    def delete_group(self, group_id, commit_changes=True):
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
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Get group information from the database to have access to the
        # identifier of the associated workflow. If the result is None we
        # assume that the group does not exist and raise an error.
        sql = 'SELECT workflow_id FROM workflow_group WHERE group_id = ?'
        row = self.con.execute(sql, (group_id,)).fetchone()
        if row is None:
            raise err.UnknownWorkflowGroupError(group_id)
        groupdir = self.fs.workflow_groupdir(row['workflow_id'], group_id)
        # Create list of SQL statements to delete all records that are
        # associated with the workflow group from the underlying database.
        stmts = list()
        stmts.append(
            'DELETE FROM run_result_file WHERE run_id IN ('
            '   SELECT r.run_id FROM workflow_run r WHERE r.group_id = ?)'
        )
        stmts.append(
            'DELETE FROM run_error_log WHERE run_id IN ('
            '   SELECT r.run_id FROM workflow_run r WHERE r.group_id = ?)'
        )
        stmts.append('DELETE FROM workflow_run WHERE group_id = ?')
        stmts.append('DELETE FROM group_member WHERE group_id  = ?')
        stmts.append('DELETE FROM group_upload_file WHERE group_id = ?')
        stmts.append('DELETE FROM workflow_group WHERE group_id = ?')
        for sql in stmts:
            self.con.execute(sql, (group_id,))
        # Commit changes only of the respective flag is True
        if commit_changes:
            self.con.commit()
        # Delete the base directory containing group files
        shutil.rmtree(groupdir)

    def get_group(self, group_id):
        """Get handle for the workflow group with the given identifier.

        Parameters
        ----------
        group_id: string
            Unique group identifier

        Returns
        -------
        flowserv.model.group.base.WorkflowGroupHandle

        Raises
        ------
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Get group information. Raise error if the identifier is unknown.
        sql = (
            'SELECT name, workflow_id, owner_id, parameters, workflow_spec '
            'FROM workflow_group '
            'WHERE group_id = ?'
        )
        row = self.con.execute(sql, (group_id,)).fetchone()
        if row is None:
            raise err.UnknownWorkflowGroupError(group_id)
        name = row['name']
        workflow_id = row['workflow_id']
        owner_id = row['owner_id']
        parameters = pb.create_parameter_index(
            json.loads(row['parameters']),
            validate=False
        )
        workflow_spec = json.loads(row['workflow_spec'])
        # Get list of team members
        members = list()
        sql = (
            'SELECT g.user_id, u.name '
            'FROM group_member g, api_user u '
            'WHERE g.user_id = u.user_id AND g.group_id = ?'
        )
        for row in self.con.execute(sql, (group_id,)).fetchall():
            user = UserHandle(identifier=row['user_id'], name=row['name'])
            members.append(user)
        # Return the group handle
        return WorkflowGroupHandle(
            con=self.con,
            identifier=group_id,
            name=name,
            workflow_id=workflow_id,
            owner_id=owner_id,
            parameters=parameters,
            workflow_spec=workflow_spec,
            fs=self.fs,
            members=members
        )

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
        list(flowserv.model.group.base.WorkflowGroupDescriptor)
        """
        # Generate SQL query depending on whether the user is given or not
        sql = 'SELECT g.group_id, g.name, g.workflow_id FROM workflow_group g'
        para = list()
        if user_id is not None:
            sql += ' WHERE g.group_id IN ('
            sql += 'SELECT m.group_id FROM group_member m WHERE m.user_id = ?)'
            para.append(user_id)
        if workflow_id is not None:
            if user_id is None:
                sql += ' WHERE '
            else:
                sql += ' AND '
            sql += 'g.workflow_id = ?'
            para.append(workflow_id)
        # Create list of group descriptors from query result
        result = list()
        for row in self.con.execute(sql, para).fetchall():
            g = WorkflowGroupDescriptor(
                identifier=row['group_id'],
                name=row['name'],
                workflow_id=row['workflow_id']
            )
            result.append(g)
        return result

    def update_group(
        self, group_id, name=None, members=None, commit_changes=True
    ):
        """Update the name and/or list of members for a workflow group.

        Parameters
        ----------
        group_id: string
            Unique group identifier
        name: string, optional
            Unique user identifier
        members: list(string), optional
            List of user identifier for group members
        commit_changes: bool, optional
            Commit changes to database only if True

        Returns
        -------
        flowserv.model.group.base.WorkflowGroupHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.UnknownUserError
        flowserv.core.error.UnknownWorkflowGroupError
        """
        # Get group handle. This will raise an error if the group is unknown.
        group = self.get_group(group_id)
        # If name and members are None we simply return the group handle.
        if name is None and members is None:
            return group
        if name is not None and name is not group.name:
            # Ensure that the given name is valid and unique for the workflow
            sql = 'SELECT name FROM workflow_group '
            sql += 'WHERE workflow_id = ? AND name = ?'
            args = (group.workflow_id, name)
            constraint.validate_name(name, con=self.con, sql=sql, args=args)
            sql = 'UPDATE workflow_group SET name = ? '
            sql += 'WHERE group_id = ?'
            self.con.execute(sql, (name, group_id))
            group.name = name
        if members is not None:
            # Create updated list of group members
            upd_members = dict()
            # Delete members that are not in the given list
            sql = 'DELETE FROM group_member '
            sql += 'WHERE group_id = ? AND user_id = ?'
            for user in group.members:
                if user.identifier not in members:
                    self.con.execute(sql, (group_id, user.identifier))
                else:
                    upd_members[user.identifier] = user
            # Add users that are not members of the group
            sqlInsMember = (
                'INSERT INTO group_member(group_id, user_id) '
                'VALUES(?, ?)'
            )
            sqlSelUser = 'SELECT name FROM api_user WHERE user_id = ?'
            for user_id in members:
                if user_id not in upd_members:
                    # Retrieve the user name to ensure that the user exists
                    row = self.con.execute(sqlSelUser, (user_id,)).fetchone()
                    if row is None:
                        raise err.UnknownUserError(user_id)
                    self.con.execute(sqlInsMember, (group_id, user_id))
                    user = UserHandle(identifier=user_id, name=row['name'])
                    upd_members[user_id] = user
            group.members = list(upd_members.values())
        if commit_changes:
            self.con.commit()
        return group
