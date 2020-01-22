# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the template repository."""

import git
import os
import pytest

from flowserv.model.template.store import TemplateRepository

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


# -- Helper class -------------------------------------------------------------
class DummyIDFunc():
    """Dummy id function."""
    def __init__(self):
        self.count = 0

    def __call__(self):
        self.count += 1
        return '0000'


# -- Unit tests ---------------------------------------------------------------

def test_add_template(tmpdir):
    """Test creating templates."""
    repo = TemplateRepository(basedir=str(tmpdir))
    t_id, template = repo.add_template(sourcedir=TEMPLATE_DIR)
    # Validate the template handle
    assert t_id is not None
    assert template.sourcedir is not None
    assert template.has_schema()
    f_spec = template.workflow_spec['inputs']['files']
    assert f_spec == ['$[[code]]', '$[[names]]']
    assert len(template.parameters) == 4
    # Make sure files are being copied
    templatedir = template.sourcedir
    assert os.path.isfile(os.path.join(templatedir, 'code/helloworld.py'))
    assert os.path.isfile(os.path.join(templatedir, 'inputs/names.txt'))
    # Force error by overriding the list of default file names
    repo = TemplateRepository(
        basedir=str(tmpdir),
        objstore=repo.objstore,
        default_filenames=['ABC']
    )
    with pytest.raises(err.InvalidTemplateError):
        repo.add_template(sourcedir=TEMPLATE_DIR)
    # Alternative specification file
    _, template = repo.add_template(
        sourcedir=TEMPLATE_DIR,
        specfile=ALT_TEMPLATE
    )
    assert not template.has_schema()
    with pytest.raises(err.UnknownParameterError):
        repo.add_template(
            sourcedir=TEMPLATE_DIR,
            specfile=ALT_TEMPLATE_ERROR
        )
    # Test error conditions
    with pytest.raises(ValueError):
        # No source given
        repo.add_template()
    with pytest.raises(ValueError):
        # Two sources given
        repo.add_template(
            sourcedir='dev/null',
            repourl='/dev/null'
        )
    with pytest.raises(git.exc.GitCommandError):
        # Invalid git repository
        repo.add_template(
            repourl='/dev/null'
        )


def test_delete_template(tmpdir):
    """Test deleting templates."""
    repo = TemplateRepository(basedir=str(tmpdir))
    t1_id, template1 = repo.add_template(sourcedir=TEMPLATE_DIR)
    t2_id, template2 = repo.add_template(sourcedir=TEMPLATE_DIR)
    assert repo.delete_template(t1_id)
    assert not repo.delete_template(t1_id)
    assert repo.delete_template(t2_id)
    assert not repo.delete_template(t2_id)


def test_error_for_id_func(tmpdir):
    """Error when the id function cannot return unique folder identifier.
    """
    dummy_func = DummyIDFunc()
    repo = TemplateRepository(basedir=str(tmpdir), idfunc=dummy_func)
    repo.add_template(sourcedir=TEMPLATE_DIR)
    with pytest.raises(RuntimeError):
        repo.add_template(sourcedir=TEMPLATE_DIR)
    assert dummy_func.count == 102


def test_get_template(tmpdir):
    """Test adding and retrieving templates."""
    repo = TemplateRepository(basedir=str(tmpdir))
    t_id, template = repo.add_template(sourcedir=TEMPLATE_DIR)
    assert template.sourcedir is not None
    assert template.has_schema()
    f_spec = template.workflow_spec['inputs']['files']
    assert f_spec == ['$[[code]]', '$[[names]]']
    assert len(template.parameters) == 4
    # Retrieve template and re-verify
    template = repo.get_template(t_id)
    assert template.has_schema()
    f_spec = template.workflow_spec['inputs']['files']
    assert f_spec == ['$[[code]]', '$[[names]]']
    assert len(template.parameters) == 4
    # Re-instantiate repository, retrieve template and re-verify
    repo = TemplateRepository(basedir=str(tmpdir))
    template = repo.get_template(t_id)
    assert template.has_schema()
    f_spec = template.workflow_spec['inputs']['files']
    assert f_spec == ['$[[code]]', '$[[names]]']
    assert len(template.parameters) == 4
    # Error for unknown template
    with pytest.raises(err.UnknownTemplateError):
        repo.get_template('unknown')
    template = repo.get_template(t_id)
    repo.delete_template(t_id)
    with pytest.raises(err.UnknownTemplateError):
        repo.get_template(t_id)
