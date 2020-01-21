# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test functionality of the template repository."""

import git
import os
import pytest

from flowserv.model.template.repo.base import TemplateRepository
from flowserv.model.template.repo.fs import TemplateFSRepository

import flowserv.core.error as err


DIR = os.path.dirname(os.path.realpath(__file__))
# Directory containing the template specification
TEMPLATE_DIR = os.path.join(DIR, '../../.files/template')
# Alternative template specification files
ALT_TEMPLATE = os.path.join(TEMPLATE_DIR, 'alt-template.yaml')
ALT_TEMPLATE_ERROR = os.path.join(TEMPLATE_DIR, 'alt-validate-error.yaml')
# Valid benchmark template
BENCHMARK = 'ABCDEFGH'
DEFAULT_NAME = 'benchmark.json'


class DummyIDFunc():
    """Dummy id function."""
    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1
        return '0000'


class TestTemplateRepository(object):
    """Test functionality of abstract template repository."""
    def test_abstract_methods(self, tmpdir):
        """Test abstract methods for completeness."""
        repo = TemplateRepository()
        with pytest.raises(NotImplementedError):
            repo.add_template('unknown')
        with pytest.raises(NotImplementedError):
            repo.delete_template('unknown')
        with pytest.raises(NotImplementedError):
            repo.exists_template('unknown')
        with pytest.raises(NotImplementedError):
            repo.get_template('unknown')
        with pytest.raises(NotImplementedError):
            repo.get_unique_identifier()


class TestTemplateFSRepository(object):
    """Test functionality of the default benchmark repository."""
    def test_add_template(self, tmpdir):
        """Test creating templates."""
        repo = TemplateFSRepository(base_dir=str(tmpdir))
        template = repo.add_template(src_dir=TEMPLATE_DIR)
        # Validate the template handle
        assert template.identifier is not None
        assert template.source_dir is not None
        assert template.has_schema()
        f_spec = template.workflow_spec['inputs']['files']
        assert f_spec == ['$[[code]]', '$[[names]]']
        assert len(template.parameters) == 4
        # Make sure files are being copied
        template_dir = repo.get_static_dir(template.identifier)
        assert os.path.isfile(os.path.join(template_dir, 'code/helloworld.py'))
        assert os.path.isfile(os.path.join(template_dir, 'inputs/names.txt'))
        # Force error by overriding the list of default file names
        repo = TemplateFSRepository(
            base_dir=str(tmpdir),
            store=repo.store,
            default_filenames=['ABC']
        )
        with pytest.raises(err.InvalidTemplateError):
            repo.add_template(src_dir=TEMPLATE_DIR)
        # Alternative specification file
        template = repo.add_template(
            src_dir=TEMPLATE_DIR,
            spec_file=ALT_TEMPLATE
        )
        assert not template.has_schema()
        with pytest.raises(err.UnknownParameterError):
            repo.add_template(
                src_dir=TEMPLATE_DIR,
                spec_file=ALT_TEMPLATE_ERROR
            )
        # Test error conditions
        with pytest.raises(ValueError):
            # No source given
            repo.add_template()
        with pytest.raises(ValueError):
            # Two sources given
            repo.add_template(
                src_dir='dev/null',
                src_repo_url='/dev/null'
            )
        with pytest.raises(git.exc.GitCommandError):
            # Invalid git repository
            repo.add_template(
                src_repo_url='/dev/null'
            )

    def test_delete_template(self, tmpdir):
        """Test deleting templates."""
        repo = TemplateFSRepository(base_dir=str(tmpdir))
        template1 = repo.add_template(src_dir=TEMPLATE_DIR)
        template2 = repo.add_template(src_dir=TEMPLATE_DIR)
        assert repo.delete_template(template1.identifier)
        assert not repo.delete_template(template1.identifier)
        assert repo.delete_template(template2.identifier)
        assert not repo.delete_template(template1.identifier)

    def test_error_for_id_func(self, tmpdir):
        """Error when the id function cannot return unique folder identifier.
        """
        dummy_func = DummyIDFunc()
        repo = TemplateFSRepository(base_dir=str(tmpdir), id_func=dummy_func)
        repo.add_template(src_dir=TEMPLATE_DIR)
        with pytest.raises(RuntimeError):
            repo.add_template(src_dir=TEMPLATE_DIR)
        assert dummy_func.count == 102

    def test_get_template(self, tmpdir):
        """Test adding and retrieving templates."""
        repo = TemplateFSRepository(base_dir=str(tmpdir))
        template = repo.add_template(src_dir=TEMPLATE_DIR)
        assert  template.source_dir is not None
        assert template.has_schema()
        f_spec = template.workflow_spec['inputs']['files']
        assert f_spec == ['$[[code]]', '$[[names]]']
        assert len(template.parameters) == 4
        # Retrieve template and re-verify
        template = repo.get_template(template.identifier)
        assert template.has_schema()
        f_spec = template.workflow_spec['inputs']['files']
        assert f_spec == ['$[[code]]', '$[[names]]']
        assert len(template.parameters) == 4
        # Re-instantiate repository, retrieve template and re-verify
        repo = TemplateFSRepository(base_dir=str(tmpdir))
        template = repo.get_template(template.identifier)
        assert template.has_schema()
        f_spec = template.workflow_spec['inputs']['files']
        assert f_spec == ['$[[code]]', '$[[names]]']
        assert len(template.parameters) == 4
        # Error for unknown template
        with pytest.raises(err.UnknownTemplateError):
            repo.get_template('unknown')
        template = repo.get_template(template.identifier)
        repo.delete_template(template.identifier)
        with pytest.raises(err.UnknownTemplateError):
            repo.get_template(template.identifier)
