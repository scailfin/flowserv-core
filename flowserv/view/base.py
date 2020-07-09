# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""base class for resource serializer."""


class Serializer(object):
    """Base serializer class. Each serializer maintains a dictionary of labels.
    Elements in the default dictionary may be overridden by a given dictionary
    of labels.
    """
    def __init__(self, labels, override_labels=None):
        """Initialize the dictionary of serialization labels.

        Parameters
        ----------
        override_labels: dict, optional
            Definition of alternative label values that override the default
            values.
        """
        self.labels = labels
        if override_labels is not None:
            # Make sure to only add labels that are already defined in the
            # default set of labels
            for key in self.labels:
                if key in override_labels:
                    self.labels[key] = override_labels[key]
