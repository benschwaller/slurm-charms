# Copyright 2025 Canonical Ltd.
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

"""Configure unit tests for the `slurmctld` charmed operator."""

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from charm import SlurmctldCharm
from constants import PEER_INTEGRATION_NAME
from ops import testing
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

if TYPE_CHECKING:
    from scenario import Manager

# A valid `slurm.jwks` key entry: exactly one key, as expected after key rotation completes.
EXAMPLE_KEY_ENTRY = {"keys": [{"alg": "HS256", "kty": "oct", "kid": "0", "k": "xyz123=="}]}


@pytest.fixture(scope="function")
def peer_integration() -> testing.PeerRelation:
    """Peer integration with a cluster name already set in the application databag."""
    return testing.PeerRelation(
        endpoint=PEER_INTEGRATION_NAME,
        interface="slurmctld-peer",
        local_app_data={"cluster_name": '"charmed-hpc"'},
    )


def patch_slurmctld_active(manager: "Manager[SlurmctldCharm]", mocker: MockerFixture) -> None:
    """Patch the `slurmctld` manager so the unit reports installed, active, and key-ready.

    This is the state the charm must be in before `check_slurmctld` reports `ActiveStatus`.
    """
    mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
    mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
    mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=EXAMPLE_KEY_ENTRY)


@pytest.fixture(scope="function")
def mock_scontrol(mocker: MockerFixture) -> MagicMock:
    """Mock the `scontrol` function from `slurm-ops`."""
    return mocker.patch("charm.scontrol")


@pytest.fixture(scope="function")
def mock_ctx() -> testing.Context[SlurmctldCharm]:
    """Mock `SlurmctldCharm` context."""
    return testing.Context(SlurmctldCharm)


@pytest.fixture(scope="function")
def mock_charm(
    mock_ctx, fs: FakeFilesystem, mocker: MockerFixture, mock_scontrol
) -> testing.Context[SlurmctldCharm]:
    """Mock `SlurmctldCharm` context with fake filesystem."""
    fs.create_file("/etc/slurm/slurm.jwks", contents='{"keys": []}', create_missing_dirs=True)
    mocker.patch("shutil.chown")  # User/group `slurm` doesn't exist on host.
    mocker.patch("subprocess.run")
    return mock_ctx


@pytest.fixture(scope="function", params=(True, False), ids=("success", "failure"))
def succeed(request: pytest.FixtureRequest) -> bool:
    """Parameterize a test to succeed and fail."""
    return request.param
