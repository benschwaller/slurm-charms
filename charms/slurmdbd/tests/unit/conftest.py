# Copyright 2025-2026 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Configure unit tests for the `slurmdbd` charmed operator."""

import pytest
from charm import SlurmdbdCharm
from constants import PEER_INTEGRATION_NAME
from ops import testing
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture


@pytest.fixture(scope="function")
def peer_integration() -> testing.PeerRelation:
    """`slurmdbd` peer integration fixture."""
    return testing.PeerRelation(
        endpoint=PEER_INTEGRATION_NAME,
        interface="slurmdbd-peer",
    )


def patch_slurmdbd_active(manager: testing.Manager[SlurmdbdCharm], mocker: MockerFixture) -> None:
    """Patch the `slurmdbd` manager so the unit reports installed and active.

    This is the state the charm must be in before `check_slurmdbd` reports `ActiveStatus`.
    """
    mocker.patch.object(manager.charm.slurmdbd, "is_installed", return_value=True)
    mocker.patch.object(manager.charm.slurmdbd.service, "is_active", return_value=True)


@pytest.fixture(scope="function")
def mock_ctx() -> testing.Context[SlurmdbdCharm]:
    """Mock `SlurmdbdCharm` context."""
    return testing.Context(SlurmdbdCharm)


@pytest.fixture(scope="function")
def mock_charm(
    mock_ctx, mocker: MockerFixture, fs: FakeFilesystem
) -> testing.Context[SlurmdbdCharm]:
    """Mock `SlurmdbdCharm` context with fake filesystem.

    Warnings:
        - The mock charm context must come before the fake filesystem fixture,
          otherwise `ops.testing.Context` will fail to locate the `slurmdbd` charm's
          charmcraft.yaml file.
    """
    fs.create_dir("/etc/slurm")
    fs.create_file("/etc/default/slurmdbd", create_missing_dirs=True)
    mocker.patch("shutil.chown")  # User/group `slurm` doesn't exist on host.
    mocker.patch("subprocess.run")
    return mock_ctx


@pytest.fixture(scope="function", params=(True, False), ids=("success", "failure"))
def succeed(request: pytest.FixtureRequest) -> bool:
    """Parameterize a test to succeed and fail."""
    return request.param
