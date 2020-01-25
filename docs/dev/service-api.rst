=============================
Overview of Service API Calls
=============================

The following is a current list of API class that are provided by the different components of the API service handle.


.. code-block:: python

    class RunService(object):
        def authorize_member(self, user, submission_id=None, run_id=None):
        def cancel_run(self, run_id, user, reason=None):
        def delete_run(self, run_id, user):
        def get_result_archive(self, run_id, user):
        def get_result_file(self, run_id, resource_id, user):
        def get_run(self, run_id, user):
        def list_runs(self, submission_id, user):
        def start_run(self, submission_id, arguments, user):


    class Service(object):
        def service_descriptor(self, username=None):


    class SubmissionService(object):
        def authorize_member(self, submission_id, user):
        def create_submission(self, benchmark_id, name, user, parameters=None, members=None):
        def delete_file(self, submission_id, file_id, user):
        def delete_submission(self, submission_id, user):
        def get_file(self, submission_id, file_id, user):
        def get_benchmark(self, benchmark_id):
        def get_submission(self, submission_id):
        def list_files(self, submission_id, user):
        def list_submissions(self, benchmark_id=None, user=None):
        def update_submission(self, submission_id, user, name=None, members=None):
        def upload_file(self, submission_id, file, file_name, user, file_type=None):


    class UserService(object):
        def activate_user(self, user_id):
        def list_users(self, query=None):
        def login_user(self, username, password):
        def logout_user(self, user):
        def register_user(self, username, password, verify=False):
        def request_password_reset(self, username):
        def reset_password(self, request_id, password):
        def whoami_user(self, user):


    class WorkflowService(object):
        def get_benchmark(self, benchmark_id):
        def get_benchmark_archive(self, benchmark_id, result_id=None):
        def get_benchmark_resource(self, benchmark_id, resource_id, result_id=None):
        def get_leaderboard(self, benchmark_id, order_by=None, include_all=False):
        def list_benchmarks(self):
