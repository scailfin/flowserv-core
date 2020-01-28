# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Interface to serialize workflow resource objects."""


import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels


class WorkflowSerializer(object):
    """Serializer for workflow resource objects. Defines the methods that are
    used to serialize workflow descriptors, handles, and listing.
    """
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: flowserv.view.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def workflow_descriptor(self, workflow):
        """Get dictionary serialization containing the descriptor of a
        workflow resource.

        Parameters
        ----------
        workflow: flowserv.model.workflow.base.WorkflowHDescriptor
            Workflow descriptor

        Returns
        -------
        dict
        """
        b_id = workflow.identifier
        leaderboard_url = self.urls.get_leaderboard(b_id)
        rel_groups_create = hateoas.action(
            hateoas.CREATE,
            resource=hateoas.GROUPS
        )
        obj = {
            labels.ID: b_id,
            labels.NAME: workflow.name,
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.get_workflow(b_id),
                hateoas.RANKING: leaderboard_url,
                rel_groups_create: self.urls.create_group(b_id)
            })
        }
        if workflow.has_description():
            obj[labels.DESCRIPTION] = workflow.description
        if workflow.has_instructions():
            obj[labels.INSTRUCTIONS] = workflow.instructions
        return obj

    def workflow_handle(self, workflow):
        """Get dictionary serialization containing the handle of a workflow
        resource.

        Parameters
        ----------
        workflow: flowserv.model.workflow.base.WorkflowHandle
            Workflow handle

        Returns
        -------
        dict
        """
        obj = self.workflow_descriptor(workflow)
        template = workflow.get_template()
        # Add parameter declarations to the serialized workflow descriptor
        parameters = template.parameters.values()
        obj[labels.PARAMETERS] = [p.to_dict() for p in parameters]
        # Add module definitions if given
        modules = template.modules
        if modules is not None:
            obj[labels.MODULES] = [
                {
                    labels.ID: m.identifier,
                    labels.NAME: m.name,
                    labels.INDEX: m.index
                } for m in modules]
        modules
        return obj

    def workflow_leaderboard(self, workflow, ranking):
        """Get dictionary serialization for a workflow evaluation leaderboard.

        Parameters
        ----------
        workflow: flowserv.model.workflow.base.WorkflowHandle
            Workflow handle
        leaderboard: flowserv.model.ranking.ResultRanking
            List of entries in the workflow evaluation leaderboard

        Returns
        -------
        dict
        """
        w_id = workflow.identifier
        # Serialize ranking entries
        entries = list()
        for run in ranking.entries:
            results = list()
            for key in run.values:
                results.append({labels.ID: key, labels.VALUE: run.values[key]})
            entries.append({
                labels.RUN: {
                    labels.ID: run.run_id,
                    labels.CREATED_AT: run.created_at.isoformat(),
                    labels.STARTED_AT: run.started_at.isoformat(),
                    labels.FINISHED_AT: run.finished_at.isoformat()
                },
                labels.GROUP: {
                    labels.ID: run.group_id,
                    labels.NAME: run.group_name
                },
                labels.RESULTS: results
            })
        # HATEOAS references
        links = {
            hateoas.SELF: self.urls.get_leaderboard(w_id),
            hateoas.WORKFLOW: self.urls.get_workflow(w_id),
        }
        # Serialize available workflow post-processing resources
        resources = list()
        current_resources = workflow.resources
        if current_resources is not None:
            result_id = current_resources.result_id
            if result_id is not None:
                for r in current_resources:
                    url = self.urls.download_workflow_resource(
                        workflow_id=w_id,
                        resource_id=r.identifier
                    )
                    resources.append({
                        labels.ID: r.identifier,
                        labels.NAME: r.name,
                        labels.CAPTION: r.caption,
                        labels.LINKS: hateoas.serialize({hateoas.SELF: url})
                    })
                archive_url = self.urls.download_workflow_archive(w_id)
                links[hateoas.RESOURCES] = archive_url
        return {
            labels.SCHEMA: [{
                    labels.ID: c.identifier,
                    labels.NAME: c.name,
                    labels.DATA_TYPE: c.data_type
                } for c in ranking.columns
            ],
            labels.RANKING: entries,
            labels.RESOURCES: resources,
            labels.LINKS: hateoas.serialize(links)
        }

    def workflow_listing(self, workflows):
        """Get dictionary serialization of a workflow listing.

        Parameters
        ----------
        workflows: list(flowserv.model.workflow.base.WorkflowDescriptor)
            List of workflow descriptors

        Returns
        -------
        dict
        """
        return {
            labels.WORKFLOWS: [
                self.workflow_descriptor(w) for w in workflows
            ],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_workflows()
            })
        }
