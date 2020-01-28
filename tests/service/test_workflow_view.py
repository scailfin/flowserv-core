# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow service API."""

import os

from flowserv.service.api import API
from flowserv.tests.controller import StateEngine

import flowserv.tests.db as db

import flowserv.core.util as util
import flowserv.model.parameter.declaration as pd
import flowserv.tests.serialize as serialize
import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

"""Define expected stes of labels in resource serializations."""
LISTING_LABELS = [labels.WORKFLOWS, labels.LINKS]
MODULE_LABELS = [labels.ID, labels.NAME, labels.INDEX]
PARA_LABELS = [
    pd.LABEL_ID,
    pd.LABEL_NAME,
    pd.LABEL_DATATYPE,
    pd.LABEL_DESCRIPTION,
    pd.LABEL_INDEX,
    pd.LABEL_REQUIRED
]
WF_LABELS_MIN = [
    labels.ID,
    labels.NAME,
    labels.LINKS,
    labels.PARAMETERS,
    labels.MODULES
]
WF_LABELS_MAX = WF_LABELS_MIN + [labels.DESCRIPTION, labels.INSTRUCTIONS]
WF_LINKS = [
    hateoas.SELF,
    hateoas.RANKING,
    hateoas.action(hateoas.CREATE, resource=hateoas.GROUPS)
]


def test_workflow_view(tmpdir):
    """Test serialization for created workflows and workflow listings."""
    # Get an API instance that uses the StateEngine as the backend
    con = db.init_db(str(tmpdir)).connect()
    engine = StateEngine()
    api = API(con=con, engine=engine)
    # Create two copies of the same workflow
    r = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    serialize.validate_links(doc=r, keys=WF_LINKS)
    util.validate_doc(doc=r, mandatory=WF_LABELS_MIN)
    r = api.workflows().get_workflow(r[labels.ID])
    serialize.validate_links(doc=r, keys=WF_LINKS)
    util.validate_doc(doc=r, mandatory=WF_LABELS_MIN)
    assert len(r[labels.MODULES]) == 1
    util.validate_doc(r[labels.MODULES][0], mandatory=MODULE_LABELS)
    assert len(r[labels.PARAMETERS]) == 3
    for para in r[labels.PARAMETERS]:
        util.validate_doc(
            doc=para,
            mandatory=PARA_LABELS,
            optional=[pd.LABEL_DEFAULT, pd.LABEL_AS]
        )
    r = api.workflows().create_workflow(
        name='W2',
        description='ABC',
        instructions='XYZ',
        sourcedir=TEMPLATE_DIR
    )
    util.validate_doc(doc=r, mandatory=WF_LABELS_MAX)
    serialize.validate_links(doc=r, keys=WF_LINKS)
    assert r[labels.DESCRIPTION] == 'ABC'
    assert r[labels.INSTRUCTIONS] == 'XYZ'
    workflow_id = r[labels.ID]
    # Workflow Listing
    r = api.workflows().list_workflows()
    util.validate_doc(doc=r, mandatory=LISTING_LABELS)
    serialize.validate_links(doc=r, keys=[hateoas.SELF])
    assert len(r[labels.WORKFLOWS]) == 2
    for wf in r[labels.WORKFLOWS]:
        util.validate_doc(
            doc=wf,
            mandatory=[labels.ID, labels.NAME,labels.LINKS],
            optional=[labels.DESCRIPTION, labels.INSTRUCTIONS]
        )
        serialize.validate_links(doc=wf, keys=WF_LINKS)
    # Delete workflow
    api.workflows().delete_workflow(workflow_id)
    r = api.workflows().list_workflows()
    assert len(r[labels.WORKFLOWS]) == 1
