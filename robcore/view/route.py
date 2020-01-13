# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Factory for Urls to access and manipulate API resources."""

import robcore.config.api as config


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
        self.benchmark_base_url = self.base_url + '/benchmarks'
        self.run_base_url = self.base_url + '/runs'
        self.submission_base_url = self.base_url + '/submissions'
        self.user_base_url = self.base_url + '/users'

    def activate_user(self):
        """Url to POST user activation request for newly registered users.

        Returns
        -------
        string
        """
        return self.user_base_url + '/activate'

    def cancel_run(self, run_id):
        """Url to POST cancel request for benchmark run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return self.get_run(run_id)

    def create_submission(self, benchmark_id):
        """Url to POST a create submission request for the given benchmark.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier

        Returns
        -------
        string
        """
        return self.list_submissions(benchmark_id=benchmark_id)

    def delete_file(self, submission_id, file_id):
        """Url to DELETE a previously uploaded file.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        string
        """
        return self.list_files(submission_id) + '/' + file_id

    def delete_run(self, run_id):
        """Url to DELETE a benchmark run.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return self.get_run(run_id)

    def delete_submission(self, submission_id):
        """Url to DELETE a submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        string
        """
        return self.get_submission(submission_id)

    def download_file(self, submission_id, file_id):
        """Url to GET a previously uploaded file.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier
        file_id: string
            Unique file identifier

        Returns
        -------
        string
        """
        return self.list_files(submission_id) + '/' + file_id

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
        return self.get_run(run_id) + '/resources/' + resource_id

    def get_benchmark(self, benchmark_id):
        """Url to GET benchmark handle.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier

        Returns
        -------
        string
        """
        return self.benchmark_base_url + '/' + benchmark_id

    def get_benchmark_resource(self, benchmark_id, resource_id):
        """Url to GET a benchmark resource.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        resource_id: string
            Unique resource identifier

        Returns
        -------
        string
        """
        return self.get_benchmark(benchmark_id) + '/resources/' + resource_id

    def get_leaderboard(self, benchmark_id, include_all=None):
        """Url to GET benchmark leaderboard.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        include_all: bool, optional
            Flag to return all results and not just one result per submission
        Returns
        -------
        string
        """
        url = self.get_benchmark(benchmark_id) + '/leaderboard'
        if not include_all is None and include_all:
            url += '?includeAll'
        return url

    def get_run(self, run_id):
        """Url to GET benchmark run handle.

        Parameters
        ----------
        run_id: string
            Unique run identifier

        Returns
        -------
        string
        """
        return self.run_base_url + '/' + run_id

    def get_submission(self, submission_id):
        """Url to GET submission handle.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        string
        """
        return self.submission_base_url + '/' + submission_id

    def list_benchmarks(self):
        """Url to GET a list of all benchmarks.

        Returns
        -------
        string
        """
        return self.benchmark_base_url

    def list_files(self, submission_id):
        """Url to GET listing of all uploaded files for a given submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        string
        """
        return self.get_submission(submission_id) + '/files'

    def list_submissions(self, benchmark_id=None):
        """Url to GET list of submissions. If the benchmark identifier is given
        a list of all submissions for the benchmark is requested. Otherwise, the
        list of all submissions that a user is a memebr of is requested.

        Parameters
        ----------
        benchmark_id: string, optional
            Unique benchmark identifier

        Returns
        -------
        string
        """
        if not benchmark_id is None:
            return self.get_benchmark(benchmark_id) + '/submissions'
        else:
            return self.submission_base_url

    def list_runs(self, submission_id):
        """Url to GET listing of benchmark runs for a given submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        string
        """
        return self.get_submission(submission_id) + '/runs'

    def list_users(self):
        """Url to GET listing of registered users.

        Returns
        -------
        string
        """
        return self.user_base_url

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

    def start_run(self, submission_id):
        """Url to POST arguments to start a new run for a given submission.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        string
        """
        return self.get_submission(submission_id) + '/runs'

    def update_submission(self, submission_id):
        """Url to PUT a submission update request.

        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        string
        """
        return self.get_submission(submission_id)

    def upload_file(self, submission_id):
        """Url to POST a new file to upload. The uploaded file is associated
        with the given submission.


        Parameters
        ----------
        submission_id: string
            Unique submission identifier

        Returns
        -------
        string
        """
        return self.list_files(submission_id)

    def whoami(self):
        """Url to GET information about a user that is logged in.

        Returns
        -------
        string
        """
        return self.user_base_url + '/whoami'
