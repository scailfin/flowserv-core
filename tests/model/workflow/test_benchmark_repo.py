# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of the benchmark repository."""

import json
import os
import pytest
import sqlite3

from flowserv.model.template.benchmark import BenchmarkHandle
from flowserv.model.template.store.benchmark import BenchmarkRepository
from flowserv.model.template.store import TemplateRepository
from flowserv.model.template.schema import ResultSchema
from flowserv.tests.repo import DictRepo

import flowserv.core.error as err
import flowserv.model.ranking as ranking
import flowserv.tests.db as db


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../../.files/benchmark/helloworld')
TEMPLATE_JSON = '../../.files/template/template.json'
TEMPLATE_WITHOUT_SCHEMA = os.path.join(DIR, TEMPLATE_JSON)
TOP_TAGGER_YAML = '../../.files/benchmark/top-tagger.yaml'
TOPTAGGER_YAML_FILE = os.path.join(DIR, TOP_TAGGER_YAML)


"""Fake template for descriptor initialization."""
TEMPLATE = dict({'A': 1})


class TestBenchmarkRepository(object):
    """Test creating and maintaining benchmarks."""
    def init(self, basedir):
        """Create empty database. Return a test instance of the benchmark
        repository and a connector to the database.
        """
        connector = db.init_db(basedir)
        repo = BenchmarkRepository(
            con=connector.connect(),
            template_repo=TemplateRepository(basedir=basedir),
            resource_basedir=os.path.join(basedir, 'resources')
        )
        return repo, connector

    def test_add_benchmark(self, tmpdir):
        """Test adding new benchmarks."""
        # Initialize the repository
        repo, connector = self.init(str(tmpdir))
        # Add benchmark with minimal information
        bm_1 = repo.add_benchmark(name='A', sourcedir=TEMPLATE_DIR)
        assert bm_1.name == 'A'
        assert not bm_1.has_description()
        assert bm_1.get_description() == ''
        assert not bm_1.has_instructions()
        assert bm_1.get_instructions() == ''
        assert bm_1.get_template().has_schema()
        # Ensure that the result table exists
        with connector.connect() as con:
            con.execute(
                'SELECT * FROM ' + ranking.RESULT_TABLE(bm_1.identifier)
            )
        # Read the result schema
        with connector.connect() as con:
            sql = 'SELECT result_schema FROM benchmark WHERE benchmark_id = ?'
            rs = con.execute(sql, (bm_1.identifier,)).fetchone()
            schema = ResultSchema.from_dict(json.loads(rs['result_schema']))
            assert schema.result_file == 'results/analytics.json'
            assert len(schema.columns) == 3
            assert len(schema.order_by) == 0
        # Template without schema
        bm_2 = repo.add_benchmark(
            name='My benchmark',
            description='desc',
            instructions='instr',
            sourcedir=TEMPLATE_DIR,
            specfile=TEMPLATE_WITHOUT_SCHEMA
        )
        assert bm_2.name == 'My benchmark'
        assert bm_2.has_description()
        assert bm_2.get_description() == 'desc'
        assert bm_2.has_instructions()
        assert bm_2.get_instructions() == 'instr'
        assert not bm_2.get_template().has_schema()
        # Result table should not exist
        with pytest.raises(sqlite3.OperationalError):
            with connector.connect() as con:
                con.execute(
                    'SELECT * FROM ' + ranking.RESULT_TABLE(bm_2.identifier)
                )
        # The result schema in the database should be none
        with connector.connect() as con:
            sql = 'SELECT result_schema FROM benchmark WHERE benchmark_id = ?'
            rs = con.execute(sql, (bm_2.identifier,)).fetchone()
            assert rs['result_schema'] is None
        # Template with post-processing step
        repo.add_benchmark(
            name='Top Tagger',
            description='desc',
            instructions='instr',
            sourcedir=TEMPLATE_DIR,
            specfile=TOPTAGGER_YAML_FILE
        )
        # Test error conditions
        # - Missing name
        with pytest.raises(err.ConstraintViolationError):
            repo.add_benchmark(name=None, sourcedir=TEMPLATE_DIR)
        with pytest.raises(err.ConstraintViolationError):
            repo.add_benchmark(name=' ', sourcedir=TEMPLATE_DIR)
        # - Invalid name
        repo.add_benchmark(name='a' * 512, sourcedir=TEMPLATE_DIR)
        with pytest.raises(err.ConstraintViolationError):
            repo.add_benchmark(name='a' * 513, sourcedir=TEMPLATE_DIR)
        # - Duplicate name
        with pytest.raises(err.ConstraintViolationError):
            repo.add_benchmark(name='My benchmark', sourcedir=TEMPLATE_DIR)
        # - No source given
        with pytest.raises(ValueError):
            repo.add_benchmark(name='A benchmark')

    def test_delete_benchmark(self, tmpdir):
        """Test deleting a benchmarks from the repository."""
        # Initialize the repository
        repo, connector = self.init(str(tmpdir))
        bm_1 = repo.add_benchmark(name='A', sourcedir=TEMPLATE_DIR)
        assert len(repo.list_benchmarks()) == 1
        bm_2 = repo.add_benchmark(
            name='My benchmark',
            description='desc',
            instructions='instr',
            sourcedir=TEMPLATE_DIR
        )
        assert len(repo.list_benchmarks()) == 2
        bm_3 = repo.add_benchmark(
            name='Another benchmark',
            sourcedir=TEMPLATE_DIR
        )
        assert len(repo.list_benchmarks()) == 3
        with pytest.raises(err.ConstraintViolationError):
            repo.add_benchmark(
                name='My benchmark',
                description='desc',
                instructions='instr',
                sourcedir=TEMPLATE_DIR
            )
        repo.delete_benchmark(bm_2.identifier)
        repo.delete_benchmark(bm_2.identifier)
        ids = [b.identifier for b in repo.list_benchmarks()]
        assert bm_1.identifier in ids
        assert bm_3.identifier in ids
        bm_2 = repo.add_benchmark(
            name='My benchmark',
            description='desc',
            instructions='instr',
            sourcedir=TEMPLATE_DIR
        )
        for bm in [bm_1, bm_2, bm_3]:
            repo.delete_benchmark(bm.identifier)
        for bm in [bm_1, bm_2, bm_3]:
            repo.delete_benchmark(bm.identifier)

    def test_get_benchmark(self, tmpdir):
        """Test retrieving benchmarks from the repository."""
        # Initialize the repository
        repo, connector = self.init(str(tmpdir))
        bm_1 = repo.add_benchmark(name='A', sourcedir=TEMPLATE_DIR)
        bm_2 = repo.add_benchmark(name='B', sourcedir=TEMPLATE_DIR)
        b_id = repo.get_benchmark(bm_1.identifier).identifier
        assert b_id == bm_1.identifier
        b_id = repo.get_benchmark(bm_2.identifier).identifier
        assert b_id == bm_2.identifier
        # Re-connect to the repository
        repo = BenchmarkRepository(
            con=connector.connect(),
            template_repo=TemplateRepository(basedir=str(tmpdir)),
            resource_basedir=os.path.join(str(tmpdir), 'resources')
        )
        benchmark = repo.get_benchmark(bm_1.identifier)
        assert benchmark.identifier == bm_1.identifier
        assert benchmark.get_resources() is None
        b_id = repo.get_benchmark(bm_2.identifier).identifier
        assert b_id == bm_2.identifier
        # Delete first benchmark
        repo.delete_benchmark(bm_1.identifier)
        with pytest.raises(err.UnknownBenchmarkError):
            repo.get_benchmark(bm_1.identifier).identifier == bm_1.identifier
        b_id = repo.get_benchmark(bm_2.identifier).identifier
        assert b_id == bm_2.identifier

    def test_init_handle(self):
        """Ensure all properties are set and errors are raised when initializing
        the handle with different sets of arguments.
        """
        # Minimal set of arguments. Uses a fake template
        b = BenchmarkHandle(identifier='A', template=TEMPLATE)
        assert b.identifier == 'A'
        assert b.name == b.identifier
        assert not b.has_description()
        assert b.get_description() == ''
        assert not b.has_instructions()
        assert b.get_instructions() == ''
        assert b.get_template() == TEMPLATE
        # Provide arguments for all parameters
        b = BenchmarkHandle(
            identifier='A',
            name='B',
            description='C',
            instructions='D',
            template=TEMPLATE,
            result_id='R',
            repo=DictRepo(templates={'A': TEMPLATE})
        )
        assert b.identifier == 'A'
        assert b.name == 'B'
        assert b.result_id == 'R'
        assert b.has_description()
        assert b.get_description() == 'C'
        assert b.has_instructions()
        assert b.get_instructions() == 'D'
        assert b.get_template() == TEMPLATE
        # Load template on demand
        b = BenchmarkHandle(
            identifier='A',
            name='B',
            description='C',
            instructions='D',
            repo=DictRepo(templates={'A': TEMPLATE})
        )
        assert b.identifier == 'A'
        assert b.name == 'B'
        assert b.has_description()
        assert b.get_description() == 'C'
        assert b.has_instructions()
        assert b.get_instructions() == 'D'
        assert b.template is None
        assert b.get_template() == TEMPLATE
        assert b.template is not None
        # Error when template and store are missing
        with pytest.raises(ValueError):
            BenchmarkHandle(
                identifier='A',
                name='B',
                description='C',
                instructions='D'
            )

    def test_update_benchmark(self, tmpdir):
        """Test updating benchmark properties."""
        # Initialize the repository
        repo, connector = self.init(str(tmpdir))
        bm_1 = repo.add_benchmark(name='A', sourcedir=TEMPLATE_DIR)
        bm_2 = repo.add_benchmark(
            name='My benchmark',
            description='desc',
            instructions='instr',
            sourcedir=TEMPLATE_DIR
        )
        # Update the name of the first benchmark. It is possible to change the
        # name to and existing name only if it is the same benchmark
        bm_1 = repo.update_benchmark(benchmark_id=bm_1.identifier, name='B')
        assert bm_1.name == 'B'
        bm_1 = repo.update_benchmark(benchmark_id=bm_1.identifier, name='B')
        assert bm_1.name == 'B'
        with pytest.raises(err.ConstraintViolationError):
            repo.update_benchmark(
                benchmark_id=bm_1.identifier,
                name='My benchmark'
            )
        # Update description and instructions
        bm_1 = repo.update_benchmark(
            benchmark_id=bm_1.identifier,
            description='My description',
            instructions='My instructions'
        )
        assert bm_1.name == 'B'
        assert bm_1.description == 'My description'
        assert bm_1.instructions == 'My instructions'
        bm_2 = repo.update_benchmark(
            benchmark_id=bm_2.identifier,
            name='The name',
            description='The description',
            instructions='The instructions'
        )
        assert bm_2.name == 'The name'
        assert bm_2.description == 'The description'
        assert bm_2.instructions == 'The instructions'
        # Do nothing
        bm_1 = repo.update_benchmark(benchmark_id=bm_1.identifier)
        assert bm_1.name == 'B'
        assert bm_1.description == 'My description'
        assert bm_1.instructions == 'My instructions'
        bm_2 = repo.update_benchmark(benchmark_id=bm_2.identifier)
        assert bm_2.name == 'The name'
        assert bm_2.description == 'The description'
        assert bm_2.instructions == 'The instructions'
