# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Simple scanner classes to collect values (e.g., for template parameters)
from different sources (e.g. standard input). The Scanner class implements the
methods that are used to convert input values into the different data types
that are supported for parameter declarations.
"""
from abc import ABCMeta, abstractmethod

from builtins import input


class Scanner(object):
    """Scanner that converts input tokens into the simple scalar data types
    boolean, float, file, integer and string that are supported in the template
    parameter declarations.
    """
    def __init__(self, reader=None):
        """Set reference to token reader. The token reader is for example
        helpful for unittest or if input is read from a file.

        By default the standard input reader is used.

        Parameters
        ----------
        reader: flowserv.scanner.TokenReader
            Reader for input tokens
        """
        self.reader = reader if reader is not None else InputReader()

    def next_bool(self, default_value=None):
        """Return next token as boolean. If the read token is an empty string
        the given default value is returned.

        Raises ValueError if the token value cannot be converted to boolean.

        Any of the following values will be recognized as valid boolean values.
        All values are case-insensitive:

        - True: [true, yes, y, t, 1]
        - False:[false, no, n, f, 0]

        Parameters
        ----------
        default_value: bool, optional
            Default value that is returned if the read token is an empty string

        Returns
        -------
        bool
        """
        val = self.reader.next_token()
        if val == '' and default_value is not None:
            return default_value
        if val.lower() in ['true', 'yes', 'y', 't', 1]:
            return True
        elif val.lower() in ['false', 'no', 'n', 'f', 0]:
            return False
        else:
            raise ValueError('not a boolean value \'' + str(val) + '\'')

    def next_file(self, default_value=None):
        """Return next token as string representing a file name. There are no
        tests performed to ensure whether the given value represents a valid
        path name or not (since the definition of a valid path name is very
        much dependent on the OS).

        Parameters
        ----------
        default_value: string, optional
            Default value that is returned if the read token is an empty string

        Returns
        -------
        string
        """
        val = self.reader.next_token().strip()
        if val == '' and default_value is not None:
            return default_value
        return val

    def next_float(self, default_value=None):
        """Return next token as float. Raises ValueError if the token value
        cannot be converted to float.

        Parameters
        ----------
        default_value: float, optional
            Default value that is returned if the read token is an empty string

        Returns
        -------
        float
        """
        val = self.reader.next_token()
        if val == '' and default_value is not None:
            return default_value
        return float(val)

    def next_int(self, default_value=None):
        """Return next token as integer. Raises ValueError if the token value
        cannot be converted to integer.

        Parameters
        ----------
        default_value: int, optional
            Default value that is returned if the read token is an empty string

        Returns
        -------
        int
        """
        val = self.reader.next_token()
        if val == '' and default_value is not None:
            return default_value
        return int(val)

    def next_string(self, default_value=None):
        """Return next token as string.

        Parameters
        ----------
        default_value: string, optional
            Default value that is returned if the read token is an empty string

        Returns
        -------
        string
        """
        val = self.reader.next_token()
        if val == '' and default_value is not None:
            return default_value
        return val


# -----------------------------------------------------------------------------
# Token Reader
# -----------------------------------------------------------------------------

class TokenReader(metaclass=ABCMeta):
    """Abstract token reader class that is used by the scanner to get the next
    input token.
    """
    @abstractmethod
    def next_token(self):
        """Read the next token.

        Returns
        -------
        string
        """
        raise NotImplementedError()


class InputReader(TokenReader):
    """Token reader that reads tokens from standard input."""
    def next_token(self):
        """Read token from standard input.

        Returns
        -------
        string
        """
        return input()


class ListReader(TokenReader):
    """Token reader that is initialized with a list of values. Returns tokens
    from the list until the end of the list is reached.
    """
    def __init__(self, tokens):
        """Initialize the list of tokens.

        Parameters
        ----------
        tokens: list(string)
            List of token values
        """
        self.tokens = tokens

    def next_token(self):
        """Return next token from the token list. If the end of the list has
        been reached None is returned.

        Returns
        -------
        string
        """
        if len(self.tokens) > 0:
            return str(self.tokens.pop(0))
        else:
            return None
