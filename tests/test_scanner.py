# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the input scanner."""

from flowserv.scanner import ListReader, Scanner


def test_read_bool():
    """Test reading Boolean values."""
    # Test reading True values.
    scanner = Scanner(reader=ListReader(tokens=['true', 'yes', 'y', 't', 1]))
    for i in range(5):
        assert scanner.next_bool()
    # Test reading False values.
    scanner = Scanner(reader=ListReader(tokens=['false', 'no', 'n', 'f', 0]))
    for i in range(5):
        assert not scanner.next_bool()
    # Test default value.
    scanner = Scanner(reader=ListReader(tokens=['', '']))
    assert scanner.next_bool(default_value=True)
    assert not scanner.next_bool(default_value=False)


def test_read_file():
    """Test reading the next file token."""
    scanner = Scanner(reader=ListReader(tokens=['out.txt', '']))
    assert scanner.next_file(default_value='a.out') == 'out.txt'
    assert scanner.next_file(default_value='a.out') == 'a.out'


def test_read_string():
    """Test reading the next file token."""
    scanner = Scanner(reader=ListReader(tokens=['a', '']))
    assert scanner.next_string(default_value='b') == 'a'
    assert scanner.next_string(default_value='b') == 'b'
    assert scanner.next_string(default_value='b') == 'b'
