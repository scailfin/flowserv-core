# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Path docker for test purposes."""

from dataclasses import dataclass

import docker
import os
import pytest


@dataclass
class FakeImage:
    tag: str

    @property
    def tags(self):
        return [self.tag]


class MockClient:
    """Mock Docker client."""
    def __init__(self):
        self._logs = None
        self._result = None

    def build(self, path, tag, nocache):
        return FakeImage(tag=tag), list()

    def close(self):
        pass

    @property
    def containers(self):
        return self

    @property
    def images(self):
        return self

    def logs(self):
        return self._logs

    def remove(self):
        pass

    def run(self, image, command, volumes, remove, environment, detach):
        """Mock run for docker container."""
        if command == 'error':
            raise docker.errors.ContainerError(
                exit_status=1,
                image=image,
                command=command,
                container=None,
                stderr='there was an error'.encode('utf-8')
            )
        elif command.startswith('papermill'):
            for dirname, spec in volumes.items():
                if spec['bind'] == '/results':
                    with open(os.path.join(dirname, 'greetings.txt'), 'wt') as f:
                        for name in ['Alice', 'Bob']:
                            f.write(f'Hey there {name}!\n')
                    pass
            msg = command
            self._result = 0
        else:
            msg, self._result = environment[command]
        self._logs = msg.encode('utf-8')
        return self

    def wait(self):
        return {'StatusCode': self._result}


@pytest.fixture
def mock_docker(monkeypatch):
    """Raise error in subprocess.run()."""

    def mock_client(*args, **kwargs):
        return MockClient()

    monkeypatch.setattr(docker, "from_env", mock_client)
