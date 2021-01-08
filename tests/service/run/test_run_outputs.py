# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for accessing run results file for a workflow with output file
specification.
"""

import os

from flowserv.config import Config
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.service.run.argument import serialize_arg, serialize_fh
from flowserv.tests.files import io_file
from flowserv.tests.service import (
    create_group, create_user, create_workflow, start_run, upload_file
)

import flowserv.model.workflow.state as st
import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
BENCHMARK_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')
BENCHMARK_FILE = os.path.join(BENCHMARK_DIR, './benchmark-outputs.yaml')


def test_run_workflow_with_outputs(local_service, tmpdir):
    """Execute the 'Hello World' example using a benchmark specification that
    includes an explicit specification of output files.
    """
    # Start a new run for the workflow template.
    config = Config().basedir(tmpdir).run_sync()
    engine = SerialWorkflowEngine(config=config)
    with local_service(config=config, engine=engine) as api:
        workflow_id = create_workflow(
            api,
            source=BENCHMARK_DIR,
            specfile=BENCHMARK_FILE
        )
        user_id = create_user(api)
    with local_service(config=config, engine=engine, user_id=user_id) as api:
        group_id = create_group(api, workflow_id)
        names = io_file(data=['Alice', 'Bob'], format='plain/text')
        file_id = upload_file(api, group_id, names)
        args = [serialize_arg('names', serialize_fh(file_id, 'data/names.txt'))]
        run_id = start_run(api, group_id, arguments=args)
    # -- Validate the run handle ----------------------------------------------
    with local_service(config=config, engine=engine, user_id=user_id) as api:
        r = api.runs().get_run(run_id)
        serialize.validate_run_handle(r, state=st.STATE_SUCCESS)
        # The run should have the greetings.txt file as a result.
        files = dict()
        for obj in r['files']:
            files[obj['name']] = obj['id']
        assert len(files) == 1
        fh = api.runs().get_result_file(
            run_id=run_id,
            file_id=files['results/greetings.txt']
        )
        value = fh.read().decode('utf-8').strip()
        assert value == 'Hello Alice!\nHello Bob!'
