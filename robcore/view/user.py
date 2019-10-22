# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Serializer for user resources."""

import robcore.view.hateoas as hateoas
import robcore.view.labels as labels


class UserSerializer(object):
    """Serializer for user resources."""
    def __init__(self, urls):
        """Initialize the reference to the Url factory.

        Parameters
        ----------
        urls: robcore.view.route.UrlFactory
            Factory for resource urls
        """
        self.urls = urls

    def registered_user(self, user):
        """Serialization for user handle of a newly registered user. The list of
        HATEOAS references will contain a link to activate the user.

        Parameters
        ----------
        user: robcore.model.user.base.UserHandle
            Handle for a registered user

        Returns
        -------
        dict
        """
        doc = self.user(user)
        link = {hateoas.action(hateoas.ACTIVATE): self.urls.activate_user()}
        doc[labels.LINKS].append(hateoas.serialize(link)[0])
        return doc

    def reset_request(self, request_id):
        """Serialization for requested identifier to rest a user password.

        Parameters
        ----------
        request_id: string
            Unique request identifier

        Returns
        -------
        dict
        """
        return {labels.REQUEST_ID: request_id}

    def user(self, user):
        """Serialization for user handle. Contains the user name and the access
        token if the user is logged in. The list of HATEOAS references will
        contain a logout link only if the user is logged in.

        Parameters
        ----------
        user: robcore.model.user.base.UserHandle
            Handle for a registered user

        Returns
        -------
        dict
        """
        doc = {labels.ID: user.identifier, labels.USERNAME: user.name}
        links = dict()
        if user.is_logged_in():
            doc[labels.ACCESS_TOKEN] = user.api_key
            links[hateoas.WHOAMI] = self.urls.whoami()
            links[hateoas.action(hateoas.LOGOUT)] = self.urls.logout()
        else:
            links[hateoas.action(hateoas.LOGIN)] = self.urls.login()
        doc[labels.LINKS] = hateoas.serialize(links)
        return doc

    def user_listing(self, users):
        """Serialize a list of user handles.

        Parameters
        ----------
        users: list(robcore.model.user.base.UserHandle)
            List of user handles

        Returns
        -------
        dict
        """
        return {
            labels.USERS: [
                {
                    labels.ID: user.identifier,
                    labels.USERNAME: user.name
                } for user in users],
            labels.LINKS: hateoas.serialize({
                hateoas.SELF: self.urls.list_users()
            })
        }
