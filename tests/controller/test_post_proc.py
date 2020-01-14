# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit-tests for executing post-processing tasks for a workflow."""

import json
import os
import pytest

import robcore.controller.postproc as postproc
import robcore.tests.api as api



DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/postproc')

USER_ID = "USER_1"


def test_prepare_directories(tmpdir):
    benchmarks, submissions, user_service, runs, _, backend, con = api.init_api(
        base_dir=str(tmpdir)
    )
    user = user_service.manager.register_user(USER_ID, USER_ID)
    benchmark = benchmarks.repo.add_benchmark(name='A', src_dir=TEMPLATE_DIR)
    # Create three submissions and execute a run for each of them
    identifiers = list()
    names = list()
    for i in range(3):
        s = submissions.create_submission(
            benchmark_id=benchmark.identifier,
            name='Team {}'.format(i),
            user=user
        )
        names.append(s['name'])
        backend.success(values={'score': i})
        r = runs.start_run(
            submission_id=s['id'],
            arguments=list(),
            user=user
        )
        identifiers.append(r['id'])
    identifiers = identifiers[::-1]
    names = names[::-1]
    leaders = benchmarks.repo.get_leaderboard(benchmark_id=benchmark.identifier)
    in_dir, out_dir = postproc.prepare_post_proc_dir(
        con=con,
        ranking=leaders,
        files=[api.RESULT_FILE]
    )
    # The submission.json file contains three entries
    with open(os.path.join(in_dir, 'submissions.json'), 'r') as f:
        metadata = json.load(f)
    assert [o['id'] for o in metadata] == identifiers
    assert [o['name'] for o in metadata] == names
    # Reverse identifiers. In this order the run score should be in increasing
    # order
    identifiers = identifiers[::-1]
    for i in range(3):
        with open(os.path.join(in_dir, identifiers[i], api.RESULT_FILE), 'r') as f:
            result = json.load(f)
            assert result == {'score': i}
