# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory for Urls to access and manipulate API resources."""

import flowserv.config.api as config


"""Name of the header eleemnt that contains the access token."""
HEADER_TOKEN = 'api_key'


class UrlFactory(object):
    """The Url factory provides methods to generate API urls to access and
    manipulate resources. For each API route there is a corresponding factory
    method to generate the respective Url.
    """
    def __init__(self, base_url=None):
        """Initialize the base Url for the service API. If the argument is not
        given the value the Url is generated from the values of the repsective
        environment variables for the server host, port, and application path.

        Parameters
        ----------
        base_url: string
            Base Url for all API resources
        """
        # Set base Url depending on whether it is given as argument or not
        if base_url is None:
            self.base_url = config.API_URL()
        else:
            self.base_url = base_url
        # Remove trailing '/' from the base url
        while self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]
        # Set base Url for resource related requests
        self.workflow_base_url = self.base_url + '/workflow'
        self.run_base_url = self.base_url + '/runs'
        self.group_base_url = self.base_url + '/groups'
        self.user_base_url = self.base_url + '/users'

    def activate_user(self):
        """Url to POST user activation request for newly registered users.

        Returns
        -------
        string
        """
        return self.user_base_url + '/activate'

    def cancel_run(self, run_id):
        """Url to POST cancel request for a workflow run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return self.get_run(run_id)

    def create_group(self, workflow_id):
        """Url to POST a create workflow group request for the given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        string
        """
        return self.list_groups(workflow_id=workflow_id)

    def delete_file(self, group_id, file_id):
        """Url to DELETE a previously uploaded file.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        string
        """
        return self.list_files(group_id) + '/' + file_id

    def delete_run(self, run_id):
        """Url to DELETE a workflow run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return self.get_run(run_id)

    def delete_group(self, group_id):
        """Url to DELETE a workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        return self.get_group(group_id)

    def download_file(self, group_id, file_id):
        """Url to GET a previously uploaded file.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        string
        """
        return self.list_files(group_id) + '/' + file_id

    def download_result_archive(self, run_id):
        """Url to GET a run result file archive.

        Parameters
        ----------
        run_ud: string
            Unique run identifier
        resource_id: string
            Unique resource identifier

        Returns
        -------
        string
        """
        # /runs/{runId}/downloads/archive
        return self.get_run(run_id) + '/downloads/archive'

    def download_result_file(self, run_id, resource_id):
        """Url to GET a run result file.

        Parameters
        ----------
        run_ud: string
            Unique run identifier
        resource_id: string
            Unique resource identifier

        Returns
        -------
        string
        """
        # /runs/{runId}/downloads/resources/{resourceId}
        url_suffix = '/downloads/resources/{}'.format(resource_id)
        return self.get_run(run_id) + url_suffix

    def get_workflow(self, workflow_id):
        """Url to GET workflow handle.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        string
        """
        return self.workflow_base_url + '/' + workflow_id

    def get_leaderboard(self, workflow_id, include_all=None):
        """Url to GET workflow leaderboard.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        include_all: bool, optional
            Flag to return all results and not just one result per workflow
            group
        Returns
        -------
        string
        """
        url = self.get_workflow(workflow_id) + '/leaderboard'
        if include_all is not None and include_all:
            url += '?includeAll'
        return url

    def get_run(self, run_id):
        """Url to GET workflow run handle.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return self.run_base_url + '/' + run_id

    def get_group(self, group_id):
        """Url to GET workflow group handle.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        return self.group_base_url + '/' + group_id

    def list_files(self, group_id):
        """Url to GET listing of all uploaded files for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        return self.get_group(group_id) + '/files'

    def list_groups(self, workflow_id=None):
        """Url to GET list of workflow groups. If theworkflow identifier is
        given a list of all groups for the workflow is requested. Otherwise,
        the list of all workflow groups the user is a member of is requested.

        Parameters
        ----------
        workflow_id: string, optional
            Unique workflow identifier

        Returns
        -------
        string
        """
        if workflow_id is not None:
            return self.get_workflow(workflow_id) + '/groups'
        else:
            return self.group_base_url

    def list_runs(self, group_id):
        """Url to GET listing of workflow runs for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        return self.get_group(group_id) + '/runs'

    def list_users(self):
        """Url to GET listing of registered users.

        Returns
        -------
        string
        """
        return self.user_base_url

    def list_workflows(self):
        """Url to GET a list of all workflows.

        Returns
        -------
        string
        """
        return self.workflow_base_url

    def login(self):
        """Url to POST user credentials for login.

        Returns
        -------
        string
        """
        return self.user_base_url + '/login'

    def logout(self):
        """Url to POST user logout request.

        Returns
        -------
        string
        """
        return self.user_base_url + '/logout'

    def register_user(self):
        """Url to POST registration request for new users.

        Returns
        -------
        string
        """
        return self.user_base_url + '/register'

    def request_password_reset(self):
        """Url to POST a password reset request.

        Returns
        -------
        string
        """
        return self.user_base_url + '/password/request'

    def reset_password(self):
        """Url to POST a new password.

        Returns
        -------
        string
        """
        return self.user_base_url + '/password/reset'

    def service_descriptor(self):
        """Url to GET the service descriptor.

        Returns
        -------
        string
        """
        return self.base_url

    def start_run(self, group_id):
        """Url to POST arguments to start a new run for a given workflow group.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        return self.get_group(group_id) + '/runs'

    def update_group(self, group_id):
        """Url to PUT a workflow group update request.

        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        return self.get_group(group_id)

    def upload_file(self, group_id):
        """Url to POST a new file to upload. The uploaded file is associated
        with the given workflow group.


        Parameters
        ----------
        group_id: string
            Unique workflow group identifier

        Returns
        -------
        string
        """
        return self.list_files(group_id)

    def whoami(self):
        """Url to GET information about a user that is logged in.

        Returns
        -------
        string
        """
        return self.user_base_url + '/whoami'
