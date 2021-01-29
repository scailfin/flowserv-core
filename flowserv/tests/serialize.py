# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to test object serialization."""

import flowserv.util as util
import flowserv.model.parameter.base as pd
import flowserv.model.workflow.state as st


# -- Files --------------------------------------------------------------------

def validate_file_handle(doc):
    """Validate serialization of a file handle.

    Parameters
    ----------
    doc: dict
        File handle serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(
        doc=doc,
        mandatory=['id', 'name', 'createdAt', 'size']
    )


def validate_file_listing(doc, count):
    """Validate serialization of a file listing. The count parameter gives the
    expected number of files in the listing.

    Parameters
    ----------
    doc: dict
        Listing of file handle serializations
    count: int
        Expected number of files in the listing

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['files'])
    assert len(doc['files']) == count
    for fh in doc['files']:
        validate_file_handle(fh)


# -- Groups -------------------------------------------------------------------

def validate_group_handle(doc):
    """Validate serialization of a workflow group handle.

    Parameters
    ----------
    doc: dict
        Workflow group handle serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(
        doc=doc,
        mandatory=['id', 'name', 'workflow', 'members', 'parameters', 'files'],
        optional=['runs']
    )


def validate_group_listing(doc):
    """Validate serialization of a workflow group listing.

    Parameters
    ----------
    doc: dict
        Listing of workflow group descriptor serializations

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['groups'])
    for g in doc['groups']:
        util.validate_doc(doc=g, mandatory=['id', 'name', 'workflow'])


# -- Rankings -----------------------------------------------------------------

def validate_ranking(doc):
    """Validate serialization of a workflow evaluation ranking.

    Parameters
    ----------
    doc: dict
        Ranking serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(
        doc=doc,
        mandatory=['schema', 'ranking'],
        optional=['postproc', 'outputs']
    )
    # Schema columns
    for col in doc['schema']:
        util.validate_doc(doc=col, mandatory=['name', 'label', 'dtype'])
    # Run results
    for entry in doc['ranking']:
        util.validate_doc(doc=entry, mandatory=['run', 'group', 'results'])
        util.validate_doc(
            doc=entry['run'],
            mandatory=['id', 'createdAt', 'startedAt', 'finishedAt']
        )
        util.validate_doc(doc=entry['group'], mandatory=['id', 'name'])
        for result in entry['results']:
            util.validate_doc(doc=result, mandatory=['name', 'value'])


# -- Runs ---------------------------------------------------------------------

def validate_run_descriptor(doc):
    """Validate serialization of  run descriptor.

    Parameters
    ----------
    doc: dict
        Run handle serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['id', 'state', 'createdAt'])


def validate_run_handle(doc, state):
    """Validate serialization of a run handle.

    Parameters
    ----------
    doc: dict
        Run handle serialization
    state: string
        Expected run state

    Raises
    ------
    ValueError
    """
    labels = ['id', 'workflowId', 'state', 'createdAt', 'arguments']
    if state == st.STATE_RUNNING:
        labels.append('startedAt')
    elif state in [st.STATE_ERROR, st.STATE_CANCELED]:
        labels.append('startedAt')
        labels.append('finishedAt')
        labels.append('messages')
    else:  # state == st.STATE_SUCCESS:
        labels.append('startedAt')
        labels.append('finishedAt')
        labels.append('files')
    util.validate_doc(
        doc=doc,
        mandatory=labels,
        optional=['parameters', 'groupId']
    )
    if 'parameters' in doc:
        for p in doc['parameters']:
            validate_parameter(p)
    assert doc['state'] == state
    if state == st.STATE_SUCCESS:
        for r in doc['files']:
            util.validate_doc(
                doc=r,
                mandatory=['id', 'name'],
                optional=['title', 'caption', 'mimeType', 'widget', 'format']
            )


def validate_run_listing(doc):
    """Validate serialization of a workflow run listing.

    Parameters
    ----------
    doc: dict
        Serialization for listing of workflow run descriptors

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['runs'])
    for r in doc['runs']:
        validate_run_descriptor(doc=r)


# -- Users --------------------------------------------------------------------

def validate_reset_request(doc):
    """Validate serialization of a user password reset request.

    Parameters
    ----------
    doc: dict
        Reset request response serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['requestId'])


def validate_user_handle(doc, login, inactive=False):
    """Validate serialization of a user handle. Serialization depends on
    whether the user is currently logged in or not.

    Parameters
    ----------
    doc: dict
        User handle serialization
    login: bool
        Flag indicating whether the handle is for a user that is logged in
    inactive: bool, optional
        Flag indicating whether the user account has been activated yet

    Raises
    ------
    ValueError
    """
    mandatory = ['id', 'username']
    if login:
        mandatory.append('token')
    util.validate_doc(doc=doc, mandatory=mandatory)


def validate_user_listing(doc):
    """Validate serialization of a user listing.

    Parameters
    ----------
    doc: dict
        Serialization for listing of user descriptors

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['users'])
    for user in doc['users']:
        util.validate_doc(doc=user, mandatory=['id', 'username'])


# -- Workflows ----------------------------------------------------------------

def validate_parameter(doc):
    """Validate serialization of a workflow parameter.

    Parameters
    ----------
    doc: dict
        Parameter serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(
        doc=doc,
        mandatory=pd.MANDATORY,
        optional=pd.OPTIONAL + ['target', 'values', 'range']
    )


def validate_para_module(doc):
    """Validate serialization of a workflow parameter module handle.

    Parameters
    ----------
    doc: dict
        Workflow parameter module handle serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['name', 'title', 'index'])


def validate_workflow_handle(doc):
    """Validate serialization of a workflow handle. Here we distinguish between
    handles that have optional elements (description and instructions) and
    those that have not.

    Parameters
    ----------
    doc: dict
        Workflow handle serialization.

    Raises
    ------
    ValueError
    """
    # Note: The parameter groups element is optional but it should be contained
    # in all local test cases. That is the reason why it is in the list of
    # mandatory elements here.
    mandatory = ['id', 'name', 'parameters', 'parameterGroups']
    util.validate_doc(
        doc=doc,
        mandatory=mandatory,
        optional=['groups', 'postproc', 'outputs']
    )
    # Validate the post-processing run handle if present
    if 'postproc' in doc:
        postproc = doc['postproc']
        validate_run_handle(doc=postproc, state=postproc['state'])


def validate_workflow_listing(doc):
    """Validate serialization of a workflow descriptor listing.

    Parameters
    ----------
    doc: dict
        Serialization for listing of workflow descriptors

    Raises
    ------
    ValueError
    """
    util.validate_doc(doc=doc, mandatory=['workflows'])
    for wf in doc['workflows']:
        util.validate_doc(
            doc=wf,
            mandatory=['id', 'name'],
            optional=['description', 'instructions']
        )
