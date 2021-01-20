# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for initializing the remote run service."""

from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.run.remote import RemoteRunService
from flowserv.view.run import RUN_ARGUMENTS


def test_custom_run_labels():
    """Test initializing the remote run service with custom labels."""
    run_service = RemoteRunService(
        descriptor=ServiceDescriptor.from_config(env=dict()),
        labels={'CANCEL_REASON': 'MY_REASONS'}
    )
    assert run_service.labels['RUN_ARGUMENTS'] == RUN_ARGUMENTS
    assert run_service.labels['CANCEL_REASON'] == 'MY_REASONS'
