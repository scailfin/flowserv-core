# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Objects for files that are created as the result of successful workflow
runs.
"""

from typing import Dict, List, Tuple

import codecs
import csv

from flowserv.model.files.base import FileHandle
from flowserv.service.api import APIFactory


class DataFile(object):
    """Basic object that represents a run result file. Provides access to the
    file content via the file handle and format-specific load methods.
    """
    def __init__(self, run_id: str, doc: Dict, service: APIFactory):
        """Initialize the file properties.

        Parameters
        ----------
        run_id: string
            Identifier of the workflow run
        doc: dict
            Serialized file handle.
        service: flowserv.client.api.APIFactory
            Factory to create instances of the service API.
        """
        self.run_id = run_id
        self.file_id = doc['id']
        self.name = doc['name']
        self.title = doc.get('title', self.name)
        self.caption = doc.get('caption')
        self.format = doc.get('format', {})
        self.service = service

    def data(self) -> Tuple[List[str], List[List[str]]]:
        """Load CSV data. Returns a list of column names and a list or rows.

        Returns
        -------
        tuple of list and list
        """
        format = self.format
        # Check if the file contains header information. Initialize the header
        # with the optional names of columns in the format descriptor.
        has_header = format.get('header', True)
        columns = format.get('columns')
        rows = list()
        # Delimiter depends on the file format.
        delim = '\t' if format['type'] == 'tsv' else ','
        f = codecs.iterdecode(self.load().open(), 'utf-8')
        for row in csv.reader(f, delimiter=delim):
            if has_header:
                # Set the has_header flag to False so that all following records
                # are added to the list of rows.
                has_header = False
                columns = row if columns is None else columns
            else:
                rows.append(row)
        columns = [None] * len(rows[0]) if not columns and rows else columns
        return (columns, rows)

    def load(self) -> FileHandle:
        """Get handle for the file.

        Returns
        -------
        flowserv.model.files.base.FileHandle
        """
        with self.service() as api:
            return api.runs().get_result_file(
                run_id=self.run_id,
                file_id=self.file_id
            )

    def text(self) -> str:
        """Read file content as string.

        Returns
        -------
        string
        """
        return self.load().open().read().decode('utf-8')
