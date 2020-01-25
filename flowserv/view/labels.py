# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Define common labels that are used on resource serializations."""

# Common resource labels
DATA_TYPE = 'type'
ID = 'id'
INDEX = 'index'
NAME = 'name'
STATE = 'state'
VERSION = 'version'
VALID_TOKEN = 'validToken'

# Timestamps
CREATED_AT = 'createdAt'
STARTED_AT = 'startedAt'
FINISHED_AT = 'finishedAt'

# Workflow resources
WORKFLOWS = 'workflows'
DESCRIPTION = 'description'
INSTRUCTIONS = 'instructions'
MODULES = 'modules'
PARAMETERS = 'parameters'
RESOURCES = 'resources'

# Workflow leaderboard
RANKING = 'ranking'
RESULTS = 'results'
RUN = 'run'
SCHEMA = 'schema'
GROUP = 'group'
VALUE = 'value'

# Benchmark runs
ARGUMENTS = 'arguments'
AS = 'as'
MESSAGES = 'messages'

# Files
FILES = 'files'
FILESIZE = 'size'

# HATEOAS
LINKS = 'links'
REF = 'href'
REL = 'rel'

# Runs
REASON = 'reason'

# Workflow groups
WORKFLOW = 'workflow'
GROUPS = 'groups'
MEMBERS = 'members'
OWNER_ID = 'ownerId'
RUNS = 'runs'

# Users
ACCESS_TOKEN = 'token'
USERNAME = 'username'
PASSWORD = 'password'
REQUEST_ID = 'requestId'
USERS = 'users'
VERIFY_USER = 'verify'
