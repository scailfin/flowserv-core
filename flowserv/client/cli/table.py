# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods and classes for the command line interface."""

from flowserv.model.parameter.numeric import NUMERIC_TYPES


class ResultTable(object):
    """Result table for database queries. Maintains a list or result rows.
    Provides functionality to format rows for printing.
    """
    def __init__(self, headline, types):
        """Initialize the list of column names and the list of column types.
        Column type identifier are the same as those used fpr template
        parameter declarations. Both lists are expected to be of same length.

        Parameters
        ----------
        headline: list(string)
            List of column names
        types: list(string)
            List of column type identifier
        """
        self.rows = list([headline])
        self.types = types

    def add(self, row):
        """Add a row to the table. The length of the row is expected to be the
        same as the length of the table headline. That is, the row contains one
        per column in the table.

        Parameters
        ----------
        row: list(string)
            List of column values
        """
        self.rows.append(row)

    def format(self):
        """Format the table rows in tabular format. Each rows is a list of
        string values. All rows are expected to have the same length. The first
        row is the header that contains the column names.

        Returns
        -------
        list(string)
        """
        # Determine the longest value for each column.
        column_size = [0] * len(self.rows[0])
        for row in self.rows:
            for col in range(len(column_size)):
                vallen = len('{}'.format(row[col]))
                if vallen > column_size[col]:
                    column_size[col] = vallen
        # Format all riws
        result = list([format_row(self.rows[0], column_size, self.types)])
        line = '-' * column_size[0]
        for i in range(1, len(row)):
            line += '-|-'
            line += '-' * column_size[i]
        result.append(line)
        for row in self.rows[1:]:
            result.append(format_row(row, column_size, self.types))
        return result


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------
def align(type_id):
    """Get align identifier depending on the data type. Numeric types are right
    aligned. All other types are left aligned.

    type_id: string
        Type identifier (from set of valid type identifier in parameter
        declarations)

    Returns
    -------
    string
    """
    if type_id in NUMERIC_TYPES:
        return '>'
    else:
        return '<'


def format_row(row, column_size, types):
    """Format the given row. Row values are padded using the given list of
    column widths.

    Parameters
    ----------
    row: list(string)
        List of cell values in a table row
    column_size: list(int)
        List of column widths
    types: list(string)
        List of column type identifier

    Returns
    -------
    string
    """
    line = '{val: {align}{width}}'.format(
        val=row[0],
        align=align(types[0]),
        width=column_size[0]
    )
    for i in range(1, len(row)):
        line += ' | '
        line += '{val: {align}{width}}'.format(
            val=row[i],
            align=align(types[i]),
            width=column_size[i]
        )
    return line
