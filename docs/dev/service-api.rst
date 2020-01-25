=============================
Overview of Service API Calls
=============================

The following is a current list of API class that are provided by the different components of the API service handle.


.. code-block:: python

    class FileService(object):
        @route('/files/{userGroupId}', methods=['GET'])
        def list_files(self, group_id, user):
            pass

        @route('/files/{userGroupId}', methods=['POST'])
        def upload_file(self, group_id, file, file_name, user, file_type=None):
            pass

        @route('/uploads/{userGroupId}/files/{fileId}', methods=['DELETE'])
        def delete_file(self, group_id, file_id, user):
            pass

        @route('/uploads/{userGroupId}/files/{fileId}', methods=['GET'])
        def get_file(self, group_id, file_id, user):
            pass


    class RunService(object):
        @route('/runs/{runId}', methods=['PUT'])
        def cancel_run(self, run_id, user, reason=None):
            pass

        @route('/runs/{runId}', methods=['DELETE'])
        def delete_run(self, run_id, user):
            pass

        @route('/runs/{runId}/downloads/archive', methods=['GET'])
        def get_result_archive(self, run_id, user):
            pass

        @route('/runs/{runId}/downloads/resources/{resourceId}', methods=['GET'])
        def get_result_file(self, run_id, resource_id, user):
            pass

        @route('/runs/{runId}', methods=['GET'])
        def get_run(self, run_id, user):
            pass

        @route('/groups/{userGroupId}/runs', methods=['GET'])
        def list_runs(self, group_id, user):
            pass

        @route('/groups/{userGroupId}/runs', methods=['POST'])
        def start_run(self, group_id, arguments, user):
            pass


    class Service(object):
        @route('/', methods=['GET'])
        def service_descriptor(self, username=None):
            pass


    class UserGroupService(object):
        @route('/workflows/{workflowId}/groups', methods=['POST'])
        def create_group(self, workflow_id, name, user, parameters=None, members=None):
            pass

        @route('/groups/{userGroupId}', methods=['DELETE'])
        def delete_group(self, group_id, user):
            pass

        @route('/groups/{userGroupId}', methods=['GET'])
        def get_group(self, group_id):
            pass

        @route('/groups', methods=['GET'])
        @route('/workflows/{workflowId}/groups', methods=['GET'])
        def list_groups(self, workflow_id=None, user=None):
            pass

        @route('/groups/{userGroupId}', methods=['GET'])
        def update_group(self, group_id, user, name=None, members=None):
            pass


    class UserService(object):
        @route('/users/activate', methods=POST)
        def activate_user(self, user_id):
            pass

        @route('/users', methods=['GET'])
        def list_users(self, query=None):
            pass

        @route('/users/login', methods=['POST'])
        def login_user(self, username, password):
            pass

        @route('/users/logout', methods=['POST'])
        def logout_user(self, user):
            pass

        @route('/users/register', methods=['POST'])
        def register_user(self, username, password, verify=False):
            pass

        @route('/users/password/request', methods=['POST'])
        def request_password_reset(self, username):
            pass

        @route('/users/password/reset', methods=['POST'])
        def reset_password(self, request_id, password):
            pass

        @route('/users/whoami', methods=['GET'])
        def whoami_user(self, user):
            pass


    class WorkflowService(object):
        @route('/workflows', methods=['POST'])
        def create_workflow(self, name, user, description=None, instructions=None, sourcedir=None, repourl=None, specfile=None):
            pass

        @route('/workflows/{workflowId}', methods=['DELETE'])
        def delete_workflow(self, workflow_id, user):
            pass

        @route('/workflows/{workflowId}', methods=['GET'])
        def get_workflow(self, workflow_id):
            pass

        @route('/workflows/{workflowId}/downloads/archive', methods=['GET'])
        def get_workflow_archive(self, workflow_id, result_id=None):
            pass

        @route('/workflows/{workflowId}/downloads/resources/{resourceId}', methods=['GET'])
        def get_workflow_resource(self, workflow_id, resource_id, result_id=None):
            pass

        @route('/workflows/{workflowId}/leaderboard', methods=['GET'])
        def get_leaderboard(self, workflow_id, order_by=None, include_all=False):
            pass

        @route('/workflows', methods=['GET'])
        def list_workflows(self):
            pass
