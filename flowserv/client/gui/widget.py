# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Simple widgets to display different types of result files from a successful
workflow run.
"""

import json
import pandas as pd
import streamlit as st

from typing import Dict, IO

from flowserv.client.app.run import Run


def display_runfiles(run: Run):
    """Display all result files for a given workflow run.

    Parameters
    ----------
    run: flowserv.app.result.RunResult
        Run result handle.
    """
    for file in run.files():
        ftype = file.format.get('type')
        f = file.load().open()
        if file.title:
            st.subheader(file.title)
        if ftype == 'csv':
            show_table(f, spec=file.format)
        elif ftype == 'image':
            show_image(f, spec=file.format)
        elif ftype == 'json':
            show_json(f, spec=file.format)
        elif ftype == 'plaintext':
            show_text(f, spec=file.format)


# -- Helper methods to display different file types. --------------------------

def show_image(file: IO, spec: Dict):
    """Display an image.

    Parameters
    ----------
    file: io.BytesIO
        IO buffer containing the file content.
    spec: dict
        File output format specification.
    """
    st.image(file, caption=spec.get('caption'))


def show_json(file: IO, spec: Dict):
    """Display a JSON object.

    Parameters
    ----------
    file: io.BytesIO
        IO buffer containing the file content.
    spec: dict
        File output format specification.
    """
    if 'caption' in spec:
        st.write(spec['caption'])
    st.write(json.load(file))


def show_table(file: IO, spec: Dict):
    """Display the contents of a CSV file as a pandas DataFrame.

    Parameters
    ----------
    file: io.BytesIO
        IO buffer containing the file content.
    spec: dict
        File output format specification.
    """
    format = spec.get('format', {})
    if not format.get('header', True) and format.get('columns'):
        df = pd.read_csv(file, header=None, names=format.get('columns'))
    else:
        df = pd.read_csv(file)
    if 'caption' in spec:
        st.write(spec['caption'])
    st.table(df)


def show_text(file: IO, spec: Dict):
    """Display the lines in a text file.

    Parameters
    ----------
    file: io.BytesIO
        IO buffer containing the file content.
    spec: dict
        File output format specification.
    """
    if 'caption' in spec:
        st.write(spec['caption'])
    for line in file:
        st.text(line.decode('utf-8'))
