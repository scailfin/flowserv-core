# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the remotefile upload service API."""

from io import StringIO

from flowserv.model.files.base import IOFile


def test_delete_file_remote(remote_service, mock_response):
    """Test deleting an uploaded file at the remote service API."""
    remote_service.uploads().delete_file(group_id='0000', file_id='0001')


def test_download_file_remote(remote_service, mock_response):
    """Test downloading a file from remote service."""
    remote_service.uploads().get_uploaded_file(group_id='0000', file_id='0001')


def test_list_files_remote(remote_service, mock_response):
    """Test listing uploaded files from the remote service."""
    remote_service.uploads().list_uploaded_files(group_id='0000')


def test_upload_file_remote(remote_service, mock_response):
    """Test uploading a file to the remote service."""
    remote_service.uploads().upload_file(
        group_id='0000',
        file=IOFile(StringIO('ABC')),
        name='file.txt'
    )
