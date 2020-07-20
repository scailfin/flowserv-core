# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

import os
import shutil
import tempfile

from flowserv.model.parameter.base import InputFile
from flowserv.model.parameter.value import TemplateArgument
from flowserv.model.template.base import WorkflowTemplate
from flowserv.model.workflow.serial import SerialWorkflow
from flowserv.model.workflow.state import StatePending

import flowserv.controller.serial.engine as serial
import flowserv.error as err
import flowserv.model.workflow.manager as repo
import flowserv.model.workflow.state as serialize
import flowserv.service.postproc.base as postbase
import flowserv.service.postproc.util as postutil
import flowserv.util as util


GITHUB_HELLOWORLD = 'https://github.com/scailfin/rob-demo-hello-world.git'


def clone_helloworld():
    """Clone 'Hello World' demo repository to temporary folder. Returns target
    folder.

    Returns
    -------
    string
    """
    return repo.git_clone(GITHUB_HELLOWORLD)


def prepare_postproc_data(templatefile, runs):
    """Prepare input data for a post-processing workflow from a given list of
    workflow runs.

    Parameters
    ----------
    templatefile: string
        Workflow template file.
    runs: list(string)
        List of path names for directories containing the runs that are
        included in the post processing.

    Returns
    -------
    string
    """
    # Read workflow templatefrom the given file.
    template = WorkflowTemplate.from_dict(
        doc=util.read_object(templatefile),
        sourcedir=os.path.dirname(templatefile),
        validate=True
    )
    postproc_spec = template.postproc_spec
    pp_inputs = postproc_spec.get('inputs', {})
    pp_files = pp_inputs.get('files', [])
    # Prepare temporary directory with result files for all
    # runs in the ranking. The created directory is the only
    # run argument.
    ranking = list()
    for run in runs:
        run_id = util.get_unique_identifier()
        ranking.append(Entry(run_id=run_id, rundir=run))
    return postutil.prepare_postproc_data(
        input_files=pp_files,
        ranking=ranking,
        run_manager=RunIndex(ranking)
    )


def run_postproc_workflow(sourcedir, runs, specfile=None, rundir=None):
    """Run post-processing workflow for a workflow template.

    Parameters
    ----------
    sourcedir: string
        Path to the base directory containing the workflow resource files.
    runs: list(string)
        List of path names for directories containing the runs that are
        included in the post processing.
    specfile: string, default=None
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)
    rundir: string, default=None
        Path to the target directory for worfklow run files. If not given, a
        temporary directory will be created.

    Returns
    -------
    flowserv.model.workflow.state.WorkflowState
    """
    rundir = rundir if rundir is not None else tempfile.mkdtemp()
    # Read workflow template and copy template files to the run directory.
    template = read_template(
        sourcedir=sourcedir,
        rundir=rundir,
        specfile=specfile
    )
    postproc_spec = template.postproc_spec
    workflow_spec = postproc_spec.get('workflow')
    pp_inputs = postproc_spec.get('inputs', {})
    pp_files = pp_inputs.get('files', [])
    # Prepare temporary directory with result files for all
    # runs in the ranking. The created directory is the only
    # run argument.
    ranking = list()
    for run in runs:
        run_id = util.get_unique_identifier()
        ranking.append(Entry(run_id=run_id, rundir=run))
    datadir = postutil.prepare_postproc_data(
        input_files=pp_files,
        ranking=ranking,
        run_manager=RunIndex(ranking)
    )
    dst = pp_inputs.get('runs', postbase.RUNS_DIR)
    runargs = {postbase.PARA_RUNS: InputFile(source=datadir, target=dst)}
    wf = SerialWorkflow(
        template=WorkflowTemplate(
            workflow_spec=workflow_spec,
            sourcedir=template.sourcedir,
            parameters=postbase.PARAMETERS
        ),
        arguments=runargs
    )
    util.copy_files(
        files=wf.upload_files(),
        target_dir=rundir,
        overwrite=False
    )
    util.create_directories(basedir=rundir, files=wf.output_files())
    _, state_dict = serial.run_workflow(
        util.get_unique_identifier(),
        rundir,
        StatePending().start(),
        wf.output_files(),
        wf.commands()
    )
    # Remove the temporary input folder
    shutil.rmtree(datadir)
    return serialize.deserialize_state(state_dict)


def run_workflow(sourcedir, arguments=dict(), specfile=None, rundir=None):
    """Run a workflow template with a given set of arguments for test purposes.
    Expects the worklfow files and specification to be located in the given
    source directory. Creates a copy of the files in the target directory. If
    no target directory is given a temporary directory is created.

    After the workflow files are copied the workflow is executed using the
    given arguments. This function will only run the main workflow but not
    any post-processing workflow that is included in the workflow template.

    Parameters
    ----------
    sourcedir: string
        Path to the base directory containing the workflow resource files.
    arguments: dict, default=dict()
        List of user provided arguments for template parameters
    specfile: string, default=None
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)
    rundir: string, default=None
        Path to the target directory for worfklow run files. If not given, a
        temporary directory will be created.

    Returns
    -------
    flowserv.model.workflow.state.WorkflowState
    """
    rundir = rundir if rundir is not None else tempfile.mkdtemp()
    # Read workflow template and copy template files to the run directory.
    template = read_template(
        sourcedir=sourcedir,
        rundir=rundir,
        specfile=specfile
    )
    # Prepare arguments for workflow run.
    runargs = dict()
    for para in template.parameters.values():
        if para.identifier in arguments:
            if para.is_file():
                fname, target_path = arguments[para.identifier]
                if target_path is None:
                    if para.has_constant():
                        if not para.as_input():
                            target_path = para.get_constant()
                    if target_path is None:
                        msg = "no target path given for '{}'"
                        raise ValueError(msg.format(para.identifier))
                val = TemplateArgument(
                    parameter=para,
                    value=InputFile(filename=fname, target_path=target_path),
                    validate=True
                )
            else:
                val = TemplateArgument(
                    parameter=para,
                    value=arguments[para.identifier],
                    validate=True
                )
            runargs[para.identifier] = val
    # Prepare workflow. Ensure to copy only those files that are not part of
    # the workflow template directory.
    wf = SerialWorkflow(template, runargs)
    util.copy_files(
        files=wf.upload_files(),
        target_dir=rundir,
        overwrite=False
    )
    util.create_directories(basedir=rundir, files=wf.output_files())
    _, state_dict = serial.run_workflow(
        util.get_unique_identifier(),
        rundir,
        StatePending().start(),
        wf.output_files(),
        wf.commands()
    )
    return serialize.deserialize_state(state_dict)


# -- Helper classes -----------------------------------------------------------

class Entry(object):
    """Entry in a virtual run ranking for post-processing. Also provides run
    handle functionality for preparing the post-processing run.
    """
    def __init__(self, run_id, rundir):
        """Initialize the run identifier and the group name (extracted from the
        run directory).
        """
        self.run_id = run_id
        self.rundir = rundir
        self.group_name = os.path.basename(rundir)

    def get_file(self, by_name):
        """Get handle for run result file with the given relative path."""
        return ResultFile(os.path.join(self.rundir, by_name))


class ResultFile(object):
    """Wrapper providing file handle functionality for run files."""
    def __init__(self, filename):
        """Initialize the file name which is the only property that is being
        accessed by the post-processing preparation code.
        """
        self.filename = filename


class RunIndex(object):
    """Run manager that provides access to post-processing input runs."""
    def __init__(self, ranking):
        """Initialize the run inex from list of ranking entries."""
        self.runs = dict()
        for entry in ranking:
            self.runs[entry.run_id] = entry

    def get_run(self, run_id):
        """Get run with the given identifier."""
        return self.runs[run_id]


# -- Helper functions ---------------------------------------------------------

def INPUTFILE(filename, target_path=None):
    """Create run argument for unpit files.

    Parameters
    ----------
    filename: string
        Path to file on disk.
    target_path: string, default=None
        Optional relative target path for file in the run directory. This is
        the user-provided location where a file is copied during run
        preparation.

    Returns
    -------
    (string, string)
    """
    return filename, target_path


def read_template(sourcedir, rundir, specfile=None):
    """Read workflow template from a given source directory.

    Parameters
    ----------
    sourcedir: string
        Path to the base directory containing the workflow resource files.
    rundir: string
        Path to the target directory for worfklow run files. If not given, a
        temporary directory will be created.
    specfile: string, default=None
        Path to the workflow template specification file (absolute or
        relative to the workflow directory)

    Returns
    -------
    flowserv.model.template.base.WorkflowTemplate
    """
    # Read project metadata from the manifest.
    projectmeta = repo.read_manifest(
        projectdir=sourcedir,
        name=util.get_short_identifier(),
        specfile=specfile
    )
    # Read the template specification file in the template workflow folder. If
    # the template is not found an error is raised.
    template = repo.read_template(
        projectmeta=projectmeta,
        projectdir=sourcedir,
        templatedir=rundir,
        default_filenames=repo.DEFAULT_TEMPLATES
    )
    if template is None:
        raise err.InvalidTemplateError('no template file found')
    # Copy files from the workflow folder to the template's static file
    # folder. By default all files in the project folder are copied.
    repo.copy_files(
        projectmeta=projectmeta,
        projectdir=sourcedir,
        templatedir=rundir
    )
    return template
