# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Classes to group template parameter for display purposes."""


"""Labels for serialized parameter group handles."""
LABEL_ID = 'id'
LABEL_INDEX = 'index'
LABEL_NAME = 'name'


class ParameterGroup(object):
    """Parameter groups are identifiable sets of parameters. These sets are
    primarily intended for display purposes in the front-end. Therefore, each
    group has a display name and an index position that defines the sort order
    for groups.
    """
    def __init__(self, identifier, name, index):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique group identifier
        name: string
            Human-readable group name
        index: int
            Group sort order index
        """
        self.identifier = identifier
        self.name = name
        self.index = index

    @staticmethod
    def from_dict(doc):
        """Create object instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for parameter group handles

        Returns
        -------
        flowserv.module.template.parameter.group.ParameterGroup

        Raises
        ------
        ValueError
        """
        util.validate_doc(
            doc,
            mandatory_labels=[LABEL_ID, LABEL_NAME, LABEL_INDEX]
        )
        return ParameterGroup(
            identifier=doc[LABEL_ID],
            name=doc[LABEL_NAME],
            index=doc[LABEL_INDEX]
        )

    def to_dict(self):
        """Get dictionary serialization for parameter group handle.

        Returns
        -------
        dict
        """
        return {
            LABEL_ID: self.identifier,
            LABEL_NAME: self.name,
            LABEL_INDEX: self.index
        }
