# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Classes to maintain of workflow output file specifications."""

import flowserv.util as util


class WorkflowOutputFile(object):
    """Information about workflow output files that are accessible as resources
    for successful workflow runs. The majority of properties that are defined
    for output files are intended to be used by applications that display the
    result of workflow runs. For each output file the following information
    may be specified:

    - source: relative path the the file in the run folder
    - title: optional title for display purposes
    - caption: optional caption for display purposes
    - mimeType: optional Mime type for the file
    - widget: optional identifier for the output widget
    - format: optional widget-specific format information.
    """
    def __init__(
        self, source, title=None, caption=None, mime_type=None, widget=None,
        format=None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        source: string
            Relative path the the file in the run folder.
        title: string, default=None
            Optional title for display purposes.
        caption: string, default=None
            Optional caption for display purposes.
        mime_type: string, default=None
            Optional Mime type for the file.
        widget: string, default=None
            Optional identifier for the output widget.
        format: dict, default=None
            Optional widget-specific format information.
        """
        self.source = source
        self.title = title
        self.caption = caption
        self.mime_type = mime_type
        self.widget = widget
        self.format = format

    @classmethod
    def from_dict(cls, doc, validate=True):
        """Create object instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for a workflow output file.
        validate: bool, default=True
            Validate the serialization if True.

        Returns
        -------
        flowserv.model.template.files.WorkflowOutputFile

        Raises
        ------
        ValueError
        """
        if validate:
            # Validate the document if the respective flag is True.
            util.validate_doc(
                doc=doc,
                mandatory=['source'],
                optional=['title', 'caption', 'mimeType', 'widget', 'format']
            )
        return cls(
            source=doc['source'],
            title=doc.get('title'),
            caption=doc.get('caption'),
            mime_type=doc.get('mimeType'),
            widget=doc.get('widget'),
            format=doc.get('format')
        )

    def to_dict(self):
        """Get dictionary serialization for the output file specification.

        Returns
        -------
        dict
        """
        doc = {'source': self.source}
        if self.title is not None:
            doc['title'] = self.title
        if self.caption is not None:
            doc['caption'] = self.caption
        if self.mime_type is not None:
            doc['mimeType'] = self.mime_type
        if self.widget is not None:
            doc['widget'] = self.widget
        if self.format is not None:
            doc['format'] = self.format
        return doc
