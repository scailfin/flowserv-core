-- This file is part of the Reproducible Open Benchmarks for Data Analysis
-- Platform (ROB).
--
-- Copyright (C) 2019 NYU.
--
-- ROB is free software; you can redistribute it and/or modify it under the
-- terms of the MIT License; see LICENSE file for more details.

-- -----------------------------------------------------------------------------
-- This file defines the database schema for the benchmark API.
--
-- All identifiers are expected to be created using the get_unique_identifier
-- method which returns string of 32 characters.
-- -----------------------------------------------------------------------------

--
-- Drop all tables (if they exist)
--
DROP TABLE IF EXISTS run_result_file;
DROP TABLE IF EXISTS run_error_log;
DROP TABLE IF EXISTS benchmark_run;
DROP TABLE IF EXISTS submission_member;
DROP TABLE IF EXISTS benchmark_submission;
DROP TABLE IF EXISTS benchmark;
DROP TABLE IF EXISTS user_key;
DROP TABLE IF EXISTS api_user;

-- Authentication --------------------------------------------------------------
--
-- The repository schema maintains only the basic information about API users.
-- Additional information (e.g., email addresses and full names) are expected
-- to be maintained by a different part of the application.
-- -----------------------------------------------------------------------------

--
-- Each API user has a unique internal identifier and a password. If the active
-- flag is 1 the user is active, otherwise the user has registered but not been
-- activated or the user been deleted. In either case an inactive user is not
-- permitted to login). Each user has a unique name. This name is the
-- identifier that is visible to the user and that is used for display purposes.
--
CREATE TABLE api_user(
    user_id CHAR(32) NOT NULL,
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
    user_id CHAR(32) NOT NULL REFERENCES api_user (user_id),
    api_key CHAR(32) NOT NULL,
    expires CHAR(26) NOT NULL,
    PRIMARY KEY(user_id),
    UNIQUE (api_key)
);

--
-- Manage requests to reset a user password.
--
CREATE TABLE password_request(
    user_id CHAR(32) NOT NULL REFERENCES api_user (user_id),
    request_id CHAR(32) NOT NULL,
    expires CHAR(26) NOT NULL,
    PRIMARY KEY(user_id),
    UNIQUE (request_id)
);

-- Benchmarks ------------------------------------------------------------------
--
-- Individual users (or teams) participate in a benchmark through submissions.
-- Each submission may be assicuated with multiple runs, representing different
-- configurations of the submission.
-- -----------------------------------------------------------------------------

--
-- Each benchmark has a unique name, a short descriptor and a set of
-- instructions. We also store the result schema information from the
-- workflow template.
--
CREATE TABLE benchmark(
    benchmark_id CHAR(32) NOT NULL,
    name VARCHAR(512) NOT NULL,
    description TEXT,
    instructions TEXT,
    result_schema TEXT,
    PRIMARY KEY(benchmark_id),
    UNIQUE(name)
);

--
-- Users participate in a benchmark through submission. Each submission has a
-- name that uniquely identifies it among all submissions for the benchmark.
-- The submission is created by a user (the owner) who can add other users as
-- members to the submission.
--
CREATE TABLE benchmark_submission(
    submission_id CHAR(32) NOT NULL,
    name VARCHAR(512) NOT NULL,
    benchmark_id CHAR(32) NOT NULL REFERENCES benchmark (benchmark_id),
    owner_id CHAR(32) NOT NULL REFERENCES api_user (user_id),
    PRIMARY KEY(submission_id),
    UNIQUE(benchmark_id, name)
);

--
-- Each file that is uploaded for a submission is assigned a unique identifier
--
CREATE TABLE submission_file(
    file_id CHAR(32) NOT NULL,
    submission_id CHAR(32) NOT NULL REFERENCES benchmark_submission (submission_id),
    name VARCHAR(512) NOT NULL,
    PRIMARY KEY(file_id)
);

--
-- Maintain information about submissions that a user participates in. There is
-- a n:m relationship between users and submissions.
--
CREATE TABLE submission_member(
    submission_id CHAR(32) NOT NULL REFERENCES benchmark_submission (submission_id),
    user_id CHAR(32) NOT NULL REFERENCES api_user (user_id),
    PRIMARY KEY(submission_id, user_id)
);

--
-- Benchmark runs maintain the run status, the provided argument values for
-- workflow parameters, and timestamps
--
CREATE TABLE benchmark_run(
    run_id CHAR(32) NOT NULL,
    submission_id CHAR(32) NOT NULL REFERENCES benchmark_submission (submission_id),
    state VARCHAR(8) NOT NULL,
    created_at CHAR(26) NOT NULL,
    started_at CHAR(26),
    ended_at CHAR(26),
    arguments TEXT NOT NULL,
    PRIMARY KEY(run_id)
);

--
-- Log for error messages for runs that are in error state.
--
CREATE TABLE run_error_log(
    run_id CHAR(32) NOT NULL REFERENCES benchmark_run (run_id),
    message TEXT NOT NULL,
    pos INTEGER NOT NULL,
    PRIMARY KEY(run_id, pos)
);

--
-- File resources that are created by successful benchmark runs.
--
CREATE TABLE run_result_file(
    run_id CHAR(32) NOT NULL REFERENCES benchmark_run (run_id),
    file_id TEXT NOT NULL,
    file_path TEXT NOT NULL,
    PRIMARY KEY(run_id, file_id)
);
