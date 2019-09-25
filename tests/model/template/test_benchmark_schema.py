# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test benchmark result schema objects."""

import pytest

import robcore.error as err
import robcore.model.template.parameter.declaration as pd
import robcore.model.template.schema as schema


class TestResultSchema(object):
    def test_column_serialization(self):
        """Test serialization of column objects."""
        # Ensure the required default value is set properly
        col = schema.ResultColumn(
            identifier='col_1',
            name='Column 1',
            data_type=pd.DT_INTEGER
        )
        assert col.identifier == 'col_1'
        assert col.name == 'Column 1'
        assert col.data_type == pd.DT_INTEGER
        assert col.required
        assert type(col.required) == bool
        col = schema.ResultColumn.from_dict(col.to_dict())
        assert col.identifier == 'col_1'
        assert col.name == 'Column 1'
        assert col.data_type == pd.DT_INTEGER
        assert col.required
        assert type(col.required) == bool
        # Test serialization if required value is given
        col = schema.ResultColumn(
            identifier='col_1',
            name='Column 1',
            data_type=pd.DT_INTEGER,
            required=False
        )
        assert col.identifier == 'col_1'
        assert col.name == 'Column 1'
        assert col.data_type == pd.DT_INTEGER
        assert not col.required
        assert type(col.required) == bool
        col = schema.ResultColumn.from_dict(col.to_dict())
        assert col.identifier == 'col_1'
        assert col.name == 'Column 1'
        assert col.data_type == pd.DT_INTEGER
        assert not col.required
        assert type(col.required) == bool

    def test_rename_columns(self):
        """Test mapping of column names to fixed naming pattern."""
        columns = [
            schema.ResultColumn(
                identifier='attr_1',
                name='Column 1',
                data_type=pd.DT_INTEGER
            ),
            schema.ResultColumn(
                identifier='attr_2',
                name='Column 2',
                data_type=pd.DT_DECIMAL,
                required=True
            ),
            schema.ResultColumn(
                identifier='attr_3',
                name='Column 3',
                data_type=pd.DT_STRING,
                required=False
            )
        ]
        s = schema.ResultSchema(columns=columns)
        # Rename with default prefix 'col'
        mapping = s.rename()
        assert len(mapping) == 3
        for key in ['attr_1', 'attr_2', 'attr_3']:
            assert key in mapping
        for key in ['col0', 'col1', 'col2']:
            assert key in mapping.values()
        # Rename with different prefix
        mapping = s.rename(prefix='a')
        assert len(mapping) == 3
        for key in ['attr_1', 'attr_2', 'attr_3']:
            assert key in mapping
        for key in ['a0', 'a1', 'a2']:
            assert key in mapping.values()
        # Rename empty schema
        mapping = schema.ResultSchema().rename()
        assert len(mapping) == 0

    def test_schema_serialization(self):
        """Test creating schema objects from dictionaries and vice versa."""
        columns = [
            schema.ResultColumn(
                identifier='col_1',
                name='Column 1',
                data_type=pd.DT_INTEGER
            ).to_dict(),
            schema.ResultColumn(
                identifier='col_2',
                name='Column 2',
                data_type=pd.DT_DECIMAL,
                required=True
            ).to_dict(),
            schema.ResultColumn(
                identifier='col_3',
                name='Column 3',
                data_type=pd.DT_STRING,
                required=False
            ).to_dict()
        ]
        s = schema.ResultSchema.from_dict({
            schema.SCHEMA_RESULTFILE: 'results.json',
            schema.SCHEMA_COLUMNS: columns
        })
        self.validate_schema(s)
        assert len(s.get_default_order()) == 1
        assert s.get_default_order()[0].sort_desc
        # Recreate the object from its serialization
        s = schema.ResultSchema.from_dict(s.to_dict())
        self.validate_schema(s)
        # Schema with ORDER BY section
        s = schema.ResultSchema.from_dict({
            schema.SCHEMA_RESULTFILE: 'results.json',
            schema.SCHEMA_COLUMNS: columns,
            schema.SCHEMA_ORDERBY: [
                schema.SortColumn(identifier='col_1').to_dict(),
                schema.SortColumn(identifier='col_2', sort_desc=False).to_dict()
            ]
        })
        self.validate_schema(s)
        assert len(s.order_by) == 2
        assert s.order_by[0].identifier == 'col_1'
        assert s.order_by[0].sort_desc
        assert s.order_by[1].identifier == 'col_2'
        assert not s.order_by[1].sort_desc
        # Recreate the object from its serialization
        s = schema.ResultSchema.from_dict(s.to_dict())
        self.validate_schema(s)
        assert len(s.order_by) == 2
        assert s.order_by[0].identifier == 'col_1'
        assert s.order_by[0].sort_desc
        assert s.order_by[1].identifier == 'col_2'
        assert not s.order_by[1].sort_desc
        # Sort column with only the identifier
        col = schema.SortColumn.from_dict({schema.SORT_ID: 'ABC'})
        assert col.identifier == 'ABC'
        assert col.sort_desc
        # Test different error cases
        # - Invalid element in dictionary
        with pytest.raises(err.InvalidTemplateError):
            doc = s.to_dict()
            doc['unknown'] = 'A'
            schema.ResultSchema.from_dict(doc)
        # - Invalid element in result column dictionary
        doc = schema.ResultColumn(
            identifier='col_1',
            name='Column 1',
            data_type=pd.DT_INTEGER
        ).to_dict()
        doc['sortOrder'] = 'DESC'
        with pytest.raises(err.InvalidTemplateError):
            schema.ResultColumn.from_dict(doc)
        # - Invalid element in sort column dictionary
        doc = schema.SortColumn(identifier='col_1').to_dict()
        doc['sortOrder'] = 'DESC'
        with pytest.raises(err.InvalidTemplateError):
            schema.SortColumn.from_dict(doc)
        # - Missing element in schema dictionary
        with pytest.raises(err.InvalidTemplateError):
            doc = s.to_dict()
            del doc[schema.SCHEMA_RESULTFILE]
            schema.ResultSchema.from_dict(doc)
        # - Missing element in result column dictionary
        doc = schema.ResultColumn(
            identifier='col_1',
            name='Column 1',
            data_type=pd.DT_INTEGER
        ).to_dict()
        del doc[schema.COLUMN_NAME]
        with pytest.raises(err.InvalidTemplateError):
            schema.ResultColumn.from_dict(doc)
        # - Missing element in sort column dictionary
        with pytest.raises(err.InvalidTemplateError):
            schema.SortColumn.from_dict({'name': 'ABC'})
        # - Invalid data type for column
        with pytest.raises(err.InvalidTemplateError):
            schema.ResultColumn(
                identifier='col_1',
                name='Column 1',
                data_type=pd.DT_LIST
            )
        # - Reference unknown attribute
        with pytest.raises(err.InvalidTemplateError):
            schema.ResultSchema.from_dict({
                schema.SCHEMA_RESULTFILE: 'results.json',
                schema.SCHEMA_COLUMNS: columns,
                schema.SCHEMA_ORDERBY: [
                    schema.SortColumn(identifier='col_1').to_dict(),
                    schema.SortColumn(identifier='col_x').to_dict()
                ]
            })

    def validate_column(self, column, identifier, name, data_type, required):
        """Ensure that the given column matches the respective arguments."""
        assert column.identifier == identifier
        assert column.name == name
        assert column.data_type == data_type
        assert column.required == required

    def validate_schema(self, schema):
        """Validate that the given schema."""
        assert len(schema.columns) == 3
        assert not schema.is_empty()
        self.validate_column(
            column=schema.columns[0],
            identifier='col_1',
            name='Column 1',
            data_type=pd.DT_INTEGER,
            required=True
        )
        self.validate_column(
            column=schema.columns[1],
            identifier='col_2',
            name='Column 2',
            data_type=pd.DT_DECIMAL,
            required=True
        )
        self.validate_column(
            column=schema.columns[2],
            identifier='col_3',
            name='Column 3',
            data_type=pd.DT_STRING,
            required=False
        )
