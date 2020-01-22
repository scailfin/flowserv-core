# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for reading arguments serial workflow templates."""

from flowserv.core.scanner import Scanner, ListReader
from flowserv.model.parameter.base import TemplateParameter, AS_INPUT
from flowserv.model.template.base import WorkflowTemplate

import flowserv.model.parameter.declaration as pd
import flowserv.model.parameter.util as tmpl


def test_read_with_record():
    """Read argument for a template that contains a parameter of data type
    DT_RECORD.
    """
    template = WorkflowTemplate(
        workflow_spec=dict(),
        sourcedir='dev/null',
        parameters=[
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='codeFile',
                    data_type=pd.DT_FILE,
                    index=0,
                    default_value=None,
                    as_const=AS_INPUT
                )
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='dataFile',
                    data_type=pd.DT_FILE,
                    index=1,
                    default_value='data/names.txt'
                )
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='resultFile',
                    data_type=pd.DT_FILE,
                    index=2,
                    default_value=None
                )
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='sleeptime',
                    data_type=pd.DT_INTEGER,
                    index=3,
                    default_value=10
                )
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='verbose',
                    data_type=pd.DT_BOOL,
                    index=4,
                    default_value=False
                )
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='frac',
                    data_type=pd.DT_DECIMAL,
                    index=6
                )
            ),
            TemplateParameter(
                pd.parameter_declaration(
                    identifier='outputType',
                    index=5
                )
            ),
        ]
    )
    sc = Scanner(reader=ListReader([
        'ABC.txt',
        'code/abc.py',
        '',
        'result/output.txt',
        'skip this error',
        3,
        True,
        'XYZ',
        0.123
    ]))
    arguments = tmpl.read(template.list_parameters(), scanner=sc)
    assert arguments['codeFile'] == ('ABC.txt', 'code/abc.py')
    assert arguments['dataFile'] == ('data/names.txt', None)
    assert arguments['resultFile'] == ('result/output.txt', None)
    assert arguments['sleeptime'] == 3
    assert arguments['verbose']
    assert arguments['outputType'] == 'XYZ'
    assert arguments['frac'] == 0.123
