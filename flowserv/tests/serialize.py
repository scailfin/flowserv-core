# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to test object serialization."""

import flowserv.view.hateoas as hateoas
import flowserv.core.util as util
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
        mandatory=['id', 'name', 'createdAt', 'size', 'links']
    )
    validate_links(doc=doc, keys=['self:download', 'self:delete'])


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
    util.validate_doc(doc=doc, mandatory=['files', 'links'])
    assert len(doc['files']) == count
    validate_links(doc=doc, keys=['self'])
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
        mandatory=[
            'id',
            'name',
            'workflow',
            'links',
            'members',
            'parameters',
            'files'
        ]
    )
    validate_links(
        doc=doc,
        keys=['self', 'workflow', 'self:upload', 'self:submit']
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
    util.validate_doc(doc=doc, mandatory=['groups', 'links'])
    validate_links(doc, keys=['self'])
    for g in doc['groups']:
        util.validate_doc(doc=g, mandatory=['id', 'name', 'workflow', 'links'])
        validate_links(
            doc=g,
            keys=['self', 'workflow', 'self:upload', 'self:submit']
        )


# -- Runs ---------------------------------------------------------------------

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
    labels = ['id', 'state', 'createdAt', 'links', 'arguments', 'parameters']
    if state == st.STATE_RUNNING:
        labels.append('startedAt')
    elif state in [st.STATE_ERROR, st.STATE_CANCELED]:
        labels.append('startedAt')
        labels.append('finishedAt')
        labels.append('messages')
    elif state == st.STATE_SUCCESS:
        labels.append('startedAt')
        labels.append('finishedAt')
        labels.append('resources')
    util.validate_doc(doc=doc, mandatory=labels)
    for p in doc['parameters']:
        validate_parameter(p)
    assert doc['state'] == state
    keys = ['self']
    if state in st.ACTIVE_STATES:
        keys.append('self:cancel')
    else:
        keys.append('self:delete')
    if state == st.STATE_SUCCESS:
        keys.append('results')
        for r in doc['resources']:
            util.validate_doc(doc=r, mandatory=['id', 'name', 'links'])
            validate_links(doc=r, keys=['self'])
    validate_links(doc=doc, keys=keys)


def validate_run_listing(doc):
    """
    """
    util.validate_doc(doc=doc, mandatory=['runs', 'links'])
    validate_links(doc=doc, keys=['self', 'submit'])
    for r in doc['runs']:
        util.validate_doc(
            doc=r,
            mandatory=['id', 'state', 'createdAt', 'links']
        )


# -- Service descriptor -------------------------------------------------------

def validate_service_descriptor(doc):
    """Validate serialization of the service descriptor.

    Parameters
    ----------
    doc: dict
        Service descriptor serialization

    Raises
    ------
    ValueError
    """
    util.validate_doc(
        doc=doc,
        mandatory=['name', 'version', 'validToken', 'links'],
        optional=['username']
    )
    validate_links(
        doc=doc,
        keys=['self', 'login', 'logout', 'register', 'workflows', 'groups']
    )


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
    mandatory = ['id', 'username', 'links']
    if login:
        mandatory.append('token')
    util.validate_doc(doc=doc, mandatory=mandatory)
    if login:
        validate_links(doc=doc, keys=['whoami', 'self:logout'])
    elif inactive:
        validate_links(doc=doc, keys=['self:activate'])
    else:
        validate_links(doc=doc, keys=['self:login'])


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
    util.validate_doc(doc=doc, mandatory=['users', 'links'])
    validate_links(doc=doc, keys=['self'])
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
        mandatory=['id', 'name', 'datatype', 'index', 'required'],
        optional=['description', 'as', 'defaultValue', 'parent', 'values']
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
    util.validate_doc(doc=doc, mandatory=['id', 'name', 'index'])


def validate_workflow_handle(doc, has_optional):
    """Validate serialization of a workflow handle. Here we distinguish between
    handles that have optional elements (description and instructions) and
    those that have not.

    Parameters
    ----------
    doc: dict
        Workflow handle serialization
    has_optional: bool
        Flag indicating whether the handle should contain description and
        instruction elements

    Raises
    ------
    ValueError
    """
    mandatory = ['id', 'name', 'links', 'parameters', 'modules']
    if has_optional:
        mandatory = mandatory + ['description', 'instructions']
    util.validate_doc(doc=doc, mandatory=mandatory)
    validate_links(doc=doc, keys=['self', 'ranking', 'groups:create'])


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
    util.validate_doc(doc=doc, mandatory=['workflows', 'links'])
    validate_links(doc=doc, keys=['self'])
    for wf in doc['workflows']:
        util.validate_doc(
            doc=wf,
            mandatory=['id', 'name', 'links'],
            optional=['description', 'instructions']
        )
        validate_links(doc=wf, keys=['self', 'ranking', 'groups:create'])


# -- Helper Functions ---------------------------------------------------------

def validate_links(doc, keys):
    """Ensure that the given list of HATEOAS references contains the mandatory
    relationship elements.

    Parameters
    ----------
    doc: dict
        Dictionary serialization of a HATEOAS reference listing
    keys: list(string)
        List of mandatory relationship keys in the reference set
    """
    # We assume that the given document contains the links key
    util.validate_doc(
        doc=hateoas.deserialize(doc['links']),
        mandatory=keys
    )
