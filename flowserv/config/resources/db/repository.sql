-- This file is part of the Reproducible and Reusable Data Analysis Workflow
-- Server (flowServ).
--
-- Copyright (C) [2019-2020] NYU.
--
-- flowServ is free software; you can redistribute it and/or modify it under the
-- terms of the MIT License; see LICENSE file for more details.

-- ----------------------------------------------------------------------------
-- This file defines the database schema for the benchmark API.
--
-- All identifiers are expected to be created using the get_unique_identifier
-- method which returns string of 32 characters.
-- ----------------------------------------------------------------------------

--
-- Drop all tables (if they exist)
--
DROP TABLE IF EXISTS run_result_file;
DROP TABLE IF EXISTS run_error_log;
DROP TABLE IF EXISTS workflow_run;
DROP TABLE IF EXISTS group_upload_file;
DROP TABLE IF EXISTS group_member;
DROP TABLE IF EXISTS workflow_group;
DROP TABLE IF EXISTS workflow_postproc;
DROP TABLE IF EXISTS workflow_template;
DROP TABLE IF EXISTS password_request;
DROP TABLE IF EXISTS user_key;
DROP TABLE IF EXISTS api_user;

-- Authentication -------------------------------------------------------------
--
-- The repository schema maintains only the basic information about API users.
-- Additional information (e.g., email addresses and full names) are expected
-- to be maintained by a different part of the application.
-- ----------------------------------------------------------------------------

--
-- Each API user has a unique internal identifier and a password. If the active
-- flag is 1 the user is active, otherwise the user has registered but not been
-- activated or the user been deleted. In either case an inactive user is not
-- permitted to login). Each user has a unique name. This name is the
-- identifier that is visible to the user and that is used for display
-- purposes.
--
CREATE TABLE api_user(
    user_id VARCHAR(32) NOT NULL,
    secret VARCHAR(512) NOT NULL,
    name VARCHAR(512) NOT NULL,
    active INTEGER NOT NULL,
    PRIMARY KEY(user_id),
    UNIQUE(name)
);

--
-- Maintain API keys for users that are currently logged in
--
CREATE TABLE user_key(
    user_id VARCHAR(32) NOT NULL REFERENCES api_user (user_id),
    api_key VARCHAR(32) NOT NULL,
    expires CHAR(26) NOT NULL,
    PRIMARY KEY(user_id),
    UNIQUE (api_key)
);

--
-- Manage requests to reset a user password.
--
CREATE TABLE password_request(
    user_id VARCHAR(32) NOT NULL REFERENCES api_user (user_id),
    request_id VARCHAR(32) NOT NULL,
    expires CHAR(26) NOT NULL,
    PRIMARY KEY(user_id),
    UNIQUE (request_id)
);

-- Workflow Template ----------------------------------------------------------
--
-- List of executable workflow templates. With each template the results of an
-- optional post-processing step over a set of workflow run results are
-- maintained.

--
-- Each workflow has a unique name, an optional short descriptor and set of
-- instructions. The five main components of the template are (i) the workflow
-- specification, (ii) the list of parameter declarations, (iii) a optional
-- post-processing workflow, (iv) the optional grouping of parameters into
-- modules, and (v) the result schema for workflows that generate metrics for
-- individual workflow runs.
-- With each workflow a reference to the latest run containing post-processing
-- results is maintained. The value is NULL if no post-porcessing workflow is
-- defined for the template or if it has not been executed yet.
--
CREATE TABLE workflow_template(
    workflow_id VARCHAR(32) NOT NULL,
    name VARCHAR(512) NOT NULL,
    description TEXT,
    instructions TEXT,
    workflow_spec TEXT NOT NULL,
    parameters TEXT,
    modules TEXT,
    postproc_spec TEXT,
    result_schema TEXT,
    postproc_id VARCHAR(32),
    PRIMARY KEY(workflow_id),
    UNIQUE(name)
);

--
-- Workflow post-processing is executed for sets of workflow runs. The set of
-- group run identifier form a unique key for the post-processing results. The
-- result key is maintained in this table. The postproc_id refers to a workflow
-- run that computed the results.
--
CREATE TABLE workflow_postproc(
    postproc_id VARCHAR(32) NOT NULL,
    workflow_id VARCHAR(32) NOT NULL REFERENCES workflow_template (workflow_id),
    group_run_id VARCHAR(32) NOT NULL,
    PRIMARY KEY(postproc_id, workflow_id, group_run_id)
);


-- Workflow Groups ------------------------------------------------------------
--
-- Groups bring together users and workflow runs. Groups are primarily intended
-- for benchmarks. In the case of a benchmark each group can be viewed as an
-- entry (or submission) to the benchmark.
-- Each group has a name that uniquely identifies it among all groups for a
-- workflow template. The group is created by a user (the owner) who can invite
-- other users as group members.
-- Each group maintains a list of uploaded files that can be used as inputs to
-- workflow runs. The different workflow runs in a group represent different
-- configurations of the workflow. When the group is defined, variations to the
-- original workflow may be made to the workflow specification and the template
-- parameter declarations.
--
CREATE TABLE workflow_group(
    group_id VARCHAR(32) NOT NULL,
    name VARCHAR(512) NOT NULL,
    workflow_id VARCHAR(32) NOT NULL REFERENCES workflow_template (workflow_id),
    owner_id VARCHAR(32) NOT NULL REFERENCES api_user (user_id),
    parameters TEXT NOT NULL,
    workflow_spec TEXT NOT NULL,
    PRIMARY KEY(group_id),
    UNIQUE(workflow_id, name)
);

--
-- Uploaded files are assigned to individual workflow groups. Each file is
-- assigned a unique identifier.
--
CREATE TABLE group_upload_file(
    file_id VARCHAR(32) NOT NULL,
    group_id VARCHAR(32) NOT NULL REFERENCES workflow_group (group_id),
    name VARCHAR(512) NOT NULL,
    file_type VARCHAR(255),
    PRIMARY KEY(file_id)
);

--
-- Maintain information about users that are members of a workflow grouping.
-- Each user can be a member of multiple groups, i.e., there is an n:m
-- relationship between users and workflow groups.
--
CREATE TABLE group_member(
    group_id VARCHAR(32) NOT NULL REFERENCES workflow_group (group_id),
    user_id VARCHAR(32) NOT NULL REFERENCES api_user (user_id),
    PRIMARY KEY(group_id, user_id)
);

-- Workflow run ---------------------------------------------------------------
--
-- Workflow runs maintain the run status, the provided argument values for
-- workflow parameters, and timestamps.
-- Workflow runs may be triggered by workflow group members or the represent
-- post-processing workflows. To be able to distinguish between these two
-- types group run information is maintained in a separate table.
--
CREATE TABLE workflow_run(
    run_id VARCHAR(32) NOT NULL,
    workflow_id VARCHAR(32) NOT NULL REFERENCES workflow_template (workflow_id),
    group_id VARCHAR(32) REFERENCES workflow_group (group_id),
    state VARCHAR(8) NOT NULL,
    created_at CHAR(26) NOT NULL,
    started_at CHAR(26),
    ended_at CHAR(26),
    arguments TEXT,
    PRIMARY KEY(run_id)
);

--
-- Log for error messages for runs that are in error state.
--
CREATE TABLE run_error_log(
    run_id VARCHAR(32) NOT NULL REFERENCES workflow_run (run_id),
    message TEXT NOT NULL,
    pos INTEGER NOT NULL,
    PRIMARY KEY(run_id, pos)
);

--
-- File resources that are created by successful workflow runs.
--
CREATE TABLE run_result_file(
    run_id VARCHAR(32) NOT NULL REFERENCES workflow_run (run_id),
    resource_id VARCHAR(32) NOT NULL,
    resource_name TEXT NOT NULL,
    PRIMARY KEY(run_id, resource_id),
    UNIQUE(run_id, resource_name)
);
