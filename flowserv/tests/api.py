# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Instantiate API service components for test purposes."""

import os

from flowserv.service.benchmark import BenchmarkService
from flowserv.service.run import RunService
from flowserv.service.submission import SubmissionService
from flowserv.service.user import UserService
from flowserv.model.user.auth import DefaultAuthPolicy, OpenAccessAuth
from flowserv.model.submission import SubmissionManager
from flowserv.model.template.repo.benchmark import BenchmarkRepository
from flowserv.model.template.repo.fs import TemplateFSRepository
from flowserv.controller.engine import BenchmarkEngine
from flowserv.model.user.base import UserManager
from flowserv.tests.benchmark import StateEngine

import flowserv.tests.db as db


RESULT_FILE = 'results/analytics.json'


def init_api(base_dir, open_access=False):
    """Initialize the database, benchmark repository, submission, and user
    manager. Returns sercive objects for benchmarks, submissions, users, runs,
    and authentication.
    """
    con = db.init_db(base_dir).connect()
    if open_access:
        auth = OpenAccessAuth(con=con)
    else:
        auth = DefaultAuthPolicy(con=con)
    controller = StateEngine(
        base_dir=os.path.join(base_dir, 'runs'),
        result_file_id=RESULT_FILE
    )
    engine = BenchmarkEngine(
        con=con,
        backend=controller
    )
    benchmark_repo = BenchmarkRepository(
        con=con,
        template_repo=TemplateFSRepository(base_dir=base_dir),
        resource_base_dir=os.path.join(base_dir, 'resources')
    )
    submission_manager = SubmissionManager(
        con=con,
        engine=engine,
        directory=base_dir
    )
    user_manager = UserManager(con=con)
    # API Services
    repository = BenchmarkService(repo=benchmark_repo)
    submissions = SubmissionService(
        engine=engine,
        manager=submission_manager,
        auth=auth,
        repo=benchmark_repo
    )
    users = UserService(manager=user_manager)
    runs = RunService(
        engine=engine,
        repo=benchmark_repo,
        submissions=submission_manager,
        auth=auth
    )
    return repository, submissions, users, runs, auth, controller, con
