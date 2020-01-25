# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The workflow API component provides methods to create and access workflows
and workflow result rankings.
"""

from flowserv.view.benchmark import BenchmarkSerializer

import floserv.core.error as err
import flowserv.core.util as util


class WorkflowService(object):
    """API component that provides methods to access workflows and workflow
    evaluation rankings (benchmark leader boards).
    """
    def __init__(self, workflow_repo, ranking_manager, urls, serializer=None):
        """Initialize the internal reference to the workflow repository, the
        ranking manager, and the route factory. The resource serializer is
        optional

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        workflow_repo: flowserv.model.workflow.repo.WorkflowRepository
            Repository to access registered workflows
        ranking_manager: flowserv.model.ranking.manager.RankingManager
            Manager for workflow evaluation rankings
        urls: flowserv.view.route.UrlFactory
            Factory for API resource Urls
        serializer: flowserv.view.benchmark.BenchmarkSerializer, optional
            Override the default serializer
        """
        self.workflow_repo = workflow_repo
        self.ranking_manager = ranking_manager
        self.urls = urls
        self.serialize = serializer
        if self.serialize is None:
            self.serialize = BenchmarkSerializer(self.urls)

    def create_workflow(
        self, name, description=None, instructions=None, sourcedir=None,
        repourl=None, specfile=None
    ):
        """Create a new workflow in the repository. If the workflow template
        includes a result schema the workflow is also registered with the
        ranking manager.

        Raises an error if the given workflow name is not unique.

        Parameters
        ----------
        name: string
            Unique workflow name
        description: string, optional
            Optional short description for display in workflow listings
        instructions: string, optional
            Text containing detailed instructions for running the workflow
        sourcedir: string, optional
            Directory containing the workflow static files and the workflow
            template specification.
        repourl: string, optional
            Git repository that contains the the workflow files
        specfile: string, optional
            Path to the workflow template specification file (absolute or
            relative to the workflow directory)

        Returns
        -------
        flowserv.model.workflow.base.WorkflowHandle

        Raises
        ------
        flowserv.core.error.ConstraintViolationError
        flowserv.core.error.InvalidTemplateError
        ValueError
        """
        # Create workflow in the repository to get the workflow handle. Do not
        # commit changes in the database yet since we may need to add the
        # ranking result table.
        workflow = self.workflow_repo.create_workflow(
            name=name,
            description=description,
            instructions=instructions,
            sourcedir=sourcedir,
            repourl=repourl,
            specfile=specfile,
            commit_changes=False
        )
        # If the workflow template contains a result schema register the
        # workflow with the ranking manager
        result_schema = workflow.get_schema()
        if result_schema is not None:
            self.ranking_manager.register_workflow(
                workflow_id=workflow.identifier,
                result_schema=result_schema,
                commit_changes=True
            )
        else:
            # Commit changes to database. Get the connection from the workflow
            # repository.
            self.workflow_repo.con.commit()
        # Return serialization og the workflow handle
        return self.serialize.benchmark_handle(workflow)

    def delete_workflow(self, workflow_id):
        """Delete the workflow with the given identifier.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Raises
        ------
        flowserv.core.error.UnknownWorkflowError
        """
        self.workflow_repo.delete_workflow(workflow_id)

    def get_leaderboard(self, workflow_id, order_by=None, include_all=False):
        """Get serialization of the evaluation leader board for the given
        workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier
        order_by: list(flowserv.model.template.schema.SortColumn), optional
            Use the given attribute to sort run results. If not given, the
            schema default sort order is used
        include_all: bool, optional
            Include all entries (True) or at most one entry (False) per user
            group in the returned ranking

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnknownWorkflowError
        """
        # Get the workflow handle to ensure that the workflow exists
        workflow = self.workflow_repo.get_workflow(workflow_id)
        # Only ifthe workflow has a defined result schema we get theranking of
        # run results. Otherwise, the ranking is empty.
        result_schema = workflow.get_schema()
        if result_schema is not None:
            ranking = self.ranking_manager.get_ranking(
                workflow_id=workflow_id,
                result_schema=result_schema,
                order_by=order_by,
                include_all=include_all
            )
        else:
            ranking = list()
        return self.serialize.benchmark_leaderboard(
            workflow=workflow,
            ranking=ranking
        )

    def get_workflow(self, workflow_id):
        """Get serialization of the handle for the given workflow.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        dict

        Raises
        ------
        flowserv.core.error.UnknownWorkflowError
        """
        # Get the workflow handle. This will ensure that the workflow exists.
        workflow = self.workflow_repo.get_workflow(workflow_id)
        return self.serialize.benchmark_handle(workflow)

    def get_workflow_archive(self, workflow_id):
        """Get in-memory compressed tar-file containing all resource files for
        a workflow that have been generated by the post-processing step. This
        method alwats returns the latest results.

        Parameters
        ----------
        workflow_id: string
            Unique workflow identifier

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.core.error.UnknownWorkflowError
        flowserv.core.error.UnknownResourceError
        """
        # Get the workflow handle. This will ensure that the workflow exists.
        workflow = self.workflow_repo.get_workflow(workflow_id)
        # Raise an error if the list of workflow post-processing resources is
        # None
        resources = workflow.resources
        if resources is None:
            raise err.UnknownResourceError('workflow evaluation archive')
        return util.targzip(resources)

    def get_workflow_resource_file(self, workflow_id, resource_id):
        """Get file handle for a benchmark resource that has been generated
        by the post-processing step. If the result identifier is None the
        latest resource results are returned.

        Parameters
        ----------
        benchmark_id: string
            Unique benchmark identifier
        resource_id: string
            Unique resource identifier
        result_id: string, optional
            Unique identifier of the post-processing result set

        Returns
        -------
        flowserv.core.files.FileHandle

        Raises
        ------
        flowserv.core.error.UnknownWorkflowError
        flowserv.core.error.UnknownResourceError
        """
        # Get the workflow handle. This will ensure that the workflow exists.
        workflow = self.workflow_repo.get_workflow(workflow_id)
        # Raise an error if the list of workflow post-processing resources is
        # None or if the specified resource does not exist.
        resources = workflow.resources
        if resources is None:
            raise err.UnknownResourceError(resource_id)
        resource = resources.get_resource(identifier=resource_id)
        if resource is None:
            raise err.UnknownResourceError(resource_id)
        return resource

    def list_workflows(self):
        """Get serialized listing of descriptors for all workflows in the
        repository.

        Returns
        -------
        dict
        """
        workflows = self.workflow_repo.list_workflows()
        return self.serialize.workflow_listing(workflows)
