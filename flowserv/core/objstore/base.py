# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Interface for classes that store dictionary objects."""

from abc import ABCMeta, abstractmethod


class ObjectStore(metaclass=ABCMeta):
    """The object store is an abstract interface to read and write dictionary
    objects from and to a simple store. Different implementations of this class
    may maintain object in persistent stores, on the file system, or keep copies
    in main memory.

    Each object in the store has a unique identifier. The identifier is not
    assigned by the store but by the component that uses the store.
    """
    @abstractmethod
    def read(self, identifier):
        """Read object with the given identifier from the store. Raises an error
        if the identifier is unknown.

        Parameters
        ----------
        identifier: string
            Unique object identifier.

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnknownObjectError
        """
        raise NotImplementedError()

    @abstractmethod
    def write(self, identifier, obj):
        """Write the given object to the store. Replaces an existing object
        with the same identifier if it exists.

        Parameters
        ----------
        identifier: string
            Unique object identifier
        obj: dict
            Object instance
        """
        raise NotImplementedError()
