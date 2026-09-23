#!/usr/bin/env python3
# Copyright 2023-2026 Canonical Ltd.
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

"""Unit tests for the `slurmdbd` charm."""

import json

import ops
import pytest
from charmed_hpc_libs.errors import SystemdError
from charmed_slurm_slurmctld_interface import JWT_KEY_LABEL
from charmed_slurm_slurmdbd_interface import AUTH_KEY_LABEL
from conftest import patch_slurmdbd_active
from constants import (
    DATABASE_INTEGRATION_NAME,
    SLURM_ACCT_DATABASE_NAME,
    SLURMDBD_INTEGRATION_NAME,
)
from ops import testing
from pytest_mock import MockerFixture
from slurm_ops import SlurmOpsError

EXAMPLE_AUTH_KEY = "xyz123=="
EXAMPLE_AUTH_KEY_ID = "12345678-90ab-cdef-1234-567890abcdef"
EXAMPLE_JWT_KEY = "-----BEGIN RSA PRIVATE KEY-----\nexample-jwt-key\n-----END RSA PRIVATE KEY-----"
EXAMPLE_MYSQL_SOCKET = "file:///var/run/mysql/mysql.sock"
EXAMPLE_DATABASE_USERNAME = "slurm"
EXAMPLE_DATABASE_PASSWORD = "slurmdbd-password"  # noqa: S105


@pytest.fixture
def auth_key_secret() -> testing.Secret:
    """Mock Slurm auth key secret."""
    return testing.Secret(
        label=AUTH_KEY_LABEL,
        tracked_content={"key": EXAMPLE_AUTH_KEY, "keyid": EXAMPLE_AUTH_KEY_ID},
    )


@pytest.fixture
def jwt_key_secret() -> testing.Secret:
    """Mock Slurm JWT key secret."""
    return testing.Secret(
        label=JWT_KEY_LABEL,
        tracked_content={"key": EXAMPLE_JWT_KEY},
    )


@pytest.mark.parametrize(
    "leader",
    (
        pytest.param(True, id="leader"),
        pytest.param(False, id="not leader"),
    ),
)
class TestSlurmdbdCharm:
    """Unit tests for the `slurmdbd` charmed operator."""

    @pytest.mark.parametrize(
        "mock_install,expected",
        (
            pytest.param(
                None,
                ops.BlockedStatus("Waiting for integrations: [`slurmctld`, `database`]"),
                id="success",
            ),
            pytest.param(
                SlurmOpsError("install failed"),
                ops.BlockedStatus(
                    "Failed to install `slurmdbd`. See `juju debug-log` for details."
                ),
                id="fail",
            ),
        ),
    )
    def test_on_install(
        self,
        mock_charm,
        mocker: MockerFixture,
        mock_install,
        leader,
        expected,
    ) -> None:
        """Test the `_on_install` event handler."""
        with mock_charm(mock_charm.on.install(), testing.State(leader=leader)) as manager:
            slurmdbd = manager.charm.slurmdbd
            mocker.patch.object(slurmdbd, "install", side_effect=mock_install)
            mocker.patch.object(slurmdbd, "is_installed")
            mocker.patch.object(slurmdbd, "version", return_value="24.05.2-1")
            mocker.patch.object(slurmdbd.service, "stop")
            mocker.patch.object(slurmdbd.service, "disable")
            mocker.patch.object(slurmdbd.service, "is_active", return_value=False)

            state = manager.run()

        if leader:
            assert state.unit_status == expected
        else:
            assert state.unit_status == ops.BlockedStatus(
                "`slurmdbd` high-availability is not supported. Scale down application"
            )

    def test_on_config_changed(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        leader,
    ) -> None:
        """Test the `_on_config_changed` event handler."""
        with mock_charm(
            mock_charm.on.config_changed(),
            testing.State(leader=leader, relations={peer_integration}),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            mocker.patch.object(slurmdbd, "is_installed", return_value=True)
            mocker.patch.object(slurmdbd.service, "is_active", return_value=False)

            manager.run()

        if leader:
            assert slurmdbd.config.path.exists()
        else:
            assert not slurmdbd.config.path.exists()

    def test_bad_configuration(self, mock_charm, peer_integration, leader) -> None:
        """Test that a bad configuration blocks the ``_on_config_changed`` event handler."""
        state = mock_charm.run(
            mock_charm.on.config_changed(),
            testing.State(
                leader=leader,
                relations={peer_integration},
                config={"slurmdbd-conf-parameters": "this is not valid slurmdbd config="},
            ),
        )

        if leader:
            assert state.unit_status == ops.BlockedStatus(
                "Configuration option(s) 'slurmdbd-conf-parameters' failed validation. "
                "See `juju debug-log` for details"
            )

    def test_reconfigure(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        leader,
        succeed,
    ) -> None:
        """Test the `_reconfigure` method."""
        slurmctld_integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME,
            interface="slurmdbd",
        )
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME,
            interface="mysql_client",
        )

        with mock_charm(
            mock_charm.on.config_changed(),
            testing.State(
                leader=leader,
                relations={peer_integration, slurmctld_integration, database_integration},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mocker.patch("charm.slurmdbd_ready", return_value=True)

            mock_reconfigure = mocker.patch.object(slurmdbd, "reconfigure")
            if not succeed:
                mock_reconfigure.side_effect = SlurmOpsError("reconfigure failed")

            state = manager.run()

        if not leader:
            mock_reconfigure.assert_not_called()
        elif succeed:
            mock_reconfigure.assert_called_once()
            assert state.unit_status == ops.ActiveStatus()
        else:
            mock_reconfigure.assert_called_once()
            assert state.unit_status == ops.BlockedStatus(
                "Failed to apply updated `slurmdbd` configuration. "
                "See `juju debug-log` for details"
            )

    def test_on_slurmctld_ready(
        self, mock_charm, mocker: MockerFixture, peer_integration, leader
    ) -> None:
        """Test that `_on_slurmctld_ready` writes controller keys and reconfigures `slurmdbd`.

        Verifies that both the auth key and JWT key are pulled from the `slurmctld`
        integration data, written to the unit, and that the service is reconfigured once
        all readiness conditions are satisfied.
        """
        auth_key_secret = testing.Secret(
            tracked_content={"key": EXAMPLE_AUTH_KEY, "keyid": EXAMPLE_AUTH_KEY_ID}
        )
        jwt_key_secret = testing.Secret(tracked_content={"key": EXAMPLE_JWT_KEY})

        integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME,
            interface="slurmdbd",
            remote_app_name="slurmctld",
            remote_app_data={
                "auth_secret_id": json.dumps(auth_key_secret.id),
                "jwt_secret_id": json.dumps(jwt_key_secret.id),
            },
        )
        # Populated so that the real `slurmdbd_ready` check - rather than a mocked one -
        # gates whether the service is reconfigured.
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME,
            interface="mysql_client",
            remote_app_name="mysql",
            remote_app_data={
                "database": SLURM_ACCT_DATABASE_NAME,
                "endpoints": "10.0.0.1:3306",
                "username": EXAMPLE_DATABASE_USERNAME,
                "password": EXAMPLE_DATABASE_PASSWORD,
            },
        )

        with mock_charm(
            mock_charm.on.relation_changed(integration),
            testing.State(
                leader=leader,
                relations={peer_integration, integration, database_integration},
                secrets={auth_key_secret, jwt_key_secret},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mock_reconfigure = mocker.patch.object(slurmdbd, "reconfigure")

            state = manager.run()

        if leader:
            mock_reconfigure.assert_called_once()
            assert json.loads(slurmdbd.key.path.read_text()) == {
                "keys": [
                    {
                        "alg": "HS256",
                        "kty": "oct",
                        "kid": EXAMPLE_AUTH_KEY_ID,
                        "k": EXAMPLE_AUTH_KEY,
                    }
                ]
            }
            assert slurmdbd.jwt.path.read_text() == EXAMPLE_JWT_KEY
            assert state.unit_status == ops.ActiveStatus()
        else:
            mock_reconfigure.assert_not_called()
            assert not slurmdbd.key.path.exists()
            assert not slurmdbd.jwt.path.exists()

    def test_on_secret_changed_auth_key(
        self, mock_charm, mocker: MockerFixture, peer_integration, leader, auth_key_secret
    ) -> None:
        """Test that `_on_secret_changed` writes the rotated auth key and restarts `slurmdbd`.

        `slurmdbd` cannot reload key files via `SIGHUP`, so the service must be restarted
        (not reloaded) for a rotated auth key to take effect.
        """
        slurmctld_integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME, interface="slurmdbd"
        )
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME, interface="mysql_client"
        )

        with mock_charm(
            mock_charm.on.secret_changed(auth_key_secret),
            testing.State(
                leader=leader,
                relations={peer_integration, slurmctld_integration, database_integration},
                secrets={auth_key_secret},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mock_restart = mocker.patch.object(slurmdbd.service, "restart")

            state = manager.run()

        assert json.loads(slurmdbd.key.path.read_text()) == {
            "keys": [
                {"alg": "HS256", "kty": "oct", "kid": EXAMPLE_AUTH_KEY_ID, "k": EXAMPLE_AUTH_KEY}
            ]
        }
        mock_restart.assert_called_once()
        assert state.unit_status == ops.ActiveStatus()

    def test_on_secret_changed_jwt_key(
        self, mock_charm, mocker: MockerFixture, peer_integration, leader, jwt_key_secret
    ) -> None:
        """Test that `_on_secret_changed` writes the rotated JWT key and restarts `slurmdbd`."""
        slurmctld_integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME, interface="slurmdbd"
        )
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME, interface="mysql_client"
        )

        with mock_charm(
            mock_charm.on.secret_changed(jwt_key_secret),
            testing.State(
                leader=leader,
                relations={peer_integration, slurmctld_integration, database_integration},
                secrets={jwt_key_secret},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mock_restart = mocker.patch.object(slurmdbd.service, "restart")

            state = manager.run()

        assert slurmdbd.jwt.path.read_text() == EXAMPLE_JWT_KEY
        mock_restart.assert_called_once()
        assert state.unit_status == ops.ActiveStatus()

    def test_on_secret_changed_unrelated_secret(
        self, mock_charm, mocker: MockerFixture, leader
    ) -> None:
        """Test that `_on_secret_changed` ignores secrets unrelated to `slurmdbd`.

        An unrelated secret changing must not restart the `slurmdbd` service.
        """
        secret = testing.Secret(label="some-unrelated-secret", tracked_content={"key": "value"})

        with mock_charm(
            mock_charm.on.secret_changed(secret),
            testing.State(leader=leader, secrets={secret}),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            mocker.patch.object(slurmdbd, "is_installed", return_value=True)
            mock_restart = mocker.patch.object(slurmdbd.service, "restart")

            manager.run()

        mock_restart.assert_not_called()
        assert not slurmdbd.key.path.exists()
        assert not slurmdbd.jwt.path.exists()

    def test_on_secret_changed_invalid_auth_key(
        self, mock_charm, mocker: MockerFixture, leader
    ) -> None:
        """Test that `_on_secret_changed` blocks when the rotated auth key is invalid."""
        secret = testing.Secret(
            label=AUTH_KEY_LABEL, tracked_content={"key": EXAMPLE_AUTH_KEY, "keyid": ""}
        )

        with mock_charm(
            mock_charm.on.secret_changed(secret),
            testing.State(leader=leader, secrets={secret}),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            mocker.patch.object(slurmdbd, "is_installed", return_value=True)
            mock_restart = mocker.patch.object(slurmdbd.service, "restart")

            state = manager.run()

        mock_restart.assert_not_called()
        assert state.unit_status == ops.BlockedStatus(
            "Failed to retrieve auth key. See `juju debug-log` for details"
        )

    @pytest.mark.parametrize(
        "label,content,name",
        (
            pytest.param(
                AUTH_KEY_LABEL,
                {"key": EXAMPLE_AUTH_KEY, "keyid": EXAMPLE_AUTH_KEY_ID},
                "auth",
                id="auth key",
            ),
            pytest.param(JWT_KEY_LABEL, {"key": EXAMPLE_JWT_KEY}, "JWT", id="jwt key"),
        ),
    )
    def test_on_secret_changed_restart_failure(
        self, mock_charm, mocker: MockerFixture, peer_integration, leader, label, content, name
    ) -> None:
        """Test `_on_secret_changed` when restarting the `slurmdbd` service fails.

        Failure mode: `service.restart()` raises `SystemdError`, which is a sibling of
        `SlurmOpsError` rather than a subclass - `SlurmManager.reconfigure` wraps it, but
        this handler calls `restart` directly. If the handler does not catch it, the
        exception escapes, the unit lands in Juju `error` status instead of blocked, and
        the event is lost rather than deferred for retry. The rotated key is already on
        disk at that point, so without a retry `slurmdbd` keeps running with the old key.
        """
        secret = testing.Secret(label=label, tracked_content=content)
        slurmctld_integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME, interface="slurmdbd"
        )
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME, interface="mysql_client"
        )

        with mock_charm(
            mock_charm.on.secret_changed(secret),
            testing.State(
                leader=leader,
                relations={peer_integration, slurmctld_integration, database_integration},
                secrets={secret},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mocker.patch.object(
                slurmdbd.service, "restart", side_effect=SystemdError("restart failed")
            )

            state = manager.run()

        assert len(state.deferred) == 1
        assert state.unit_status == ops.BlockedStatus(
            f"Failed to apply new {name} key. See `juju debug-log` for details"
        )

    @pytest.mark.parametrize(
        "endpoints,expected_host,expected_port,expected_socket",
        (
            pytest.param("10.0.0.1:3306", "10.0.0.1", 3306, None, id="tcp"),
            pytest.param("[2001:db8::1]:3306", "2001:db8::1", 3306, None, id="ipv6"),
            pytest.param(
                "10.0.0.1:3306,10.0.0.2:3306", "10.0.0.1", 3306, None, id="multiple tcp endpoints"
            ),
            pytest.param(
                EXAMPLE_MYSQL_SOCKET, None, None, "/var/run/mysql/mysql.sock", id="unix socket"
            ),
            pytest.param(
                f"{EXAMPLE_MYSQL_SOCKET},10.0.0.1:3306",
                None,
                None,
                "/var/run/mysql/mysql.sock",
                id="unix socket preferred over tcp",
            ),
        ),
    )
    def test_on_database_created(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        leader,
        endpoints,
        expected_host,
        expected_port,
        expected_socket,
    ) -> None:
        """Test that `_on_database_created` writes storage config based on endpoint type.

        Socket endpoints - provided by a co-located `mysql-router` - must configure
        `MYSQL_UNIX_PORT` so the MySQL client connects over the socket. TCP endpoints must
        instead set `StorageHost`/`StoragePort`, with IPv6 brackets stripped so `slurmdbd`
        can parse the address. Socket endpoints take precedence when both are provided.
        """
        slurmctld_integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME, interface="slurmdbd"
        )
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME,
            interface="mysql_client",
            remote_app_name="mysql",
            remote_app_data={
                "database": SLURM_ACCT_DATABASE_NAME,
                "endpoints": endpoints,
                "username": EXAMPLE_DATABASE_USERNAME,
                "password": EXAMPLE_DATABASE_PASSWORD,
            },
        )

        with mock_charm(
            mock_charm.on.relation_changed(database_integration),
            testing.State(
                leader=leader,
                relations={peer_integration, slurmctld_integration, database_integration},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mocker.patch("charm.slurmdbd_ready", return_value=True)
            mock_reconfigure = mocker.patch.object(slurmdbd, "reconfigure")

            manager.run()

        if leader:
            mock_reconfigure.assert_called_once()
            storage = slurmdbd.storage.load()
            assert storage.storage_user == EXAMPLE_DATABASE_USERNAME
            assert storage.storage_pass == EXAMPLE_DATABASE_PASSWORD
            assert storage.storage_loc == SLURM_ACCT_DATABASE_NAME
            assert storage.storage_host == expected_host
            assert storage.storage_port == expected_port
            assert slurmdbd.mysql_unix_port == expected_socket
        else:
            mock_reconfigure.assert_not_called()
            assert not slurmdbd.storage.path.exists()

    def test_on_database_created_removes_stale_unix_port(
        self, mock_charm, mocker: MockerFixture, peer_integration, leader
    ) -> None:
        """Test that `_on_database_created` removes `MYSQL_UNIX_PORT` when switching to TCP.

        A stale `MYSQL_UNIX_PORT` left in `/etc/default/slurmdbd` after migrating from a
        co-located `mysql-router` socket to a remote TCP database would keep pointing the
        MySQL client at a socket that no longer exists.
        """
        slurmctld_integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME, interface="slurmdbd"
        )
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME,
            interface="mysql_client",
            remote_app_name="mysql",
            remote_app_data={
                "database": SLURM_ACCT_DATABASE_NAME,
                "endpoints": "10.0.0.1:3306",
                "username": EXAMPLE_DATABASE_USERNAME,
                "password": EXAMPLE_DATABASE_PASSWORD,
            },
        )

        with mock_charm(
            mock_charm.on.relation_changed(database_integration),
            testing.State(
                leader=leader,
                relations={peer_integration, slurmctld_integration, database_integration},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mocker.patch("charm.slurmdbd_ready", return_value=True)
            mocker.patch.object(slurmdbd, "reconfigure")

            slurmdbd.mysql_unix_port = "/var/run/mysql/mysql.sock"

            manager.run()

        if leader:
            assert slurmdbd.mysql_unix_port is None
        else:
            assert slurmdbd.mysql_unix_port == "/var/run/mysql/mysql.sock"

    @pytest.mark.parametrize(
        "endpoints",
        (
            pytest.param("", id="no endpoints"),
            pytest.param(",,", id="unusable endpoints"),
        ),
    )
    def test_on_database_created_invalid_endpoints(
        self, mock_charm, mocker: MockerFixture, peer_integration, leader, endpoints
    ) -> None:
        """Test that `_on_database_created` fails the hook on unusable endpoints.

        Unusable endpoints are an unexpected condition requiring human intervention, so the
        hook must fail loudly rather than defer - reprocessing a deferred event would only
        produce continual errors - and must not write a partial storage configuration.
        """
        slurmctld_integration = testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME, interface="slurmdbd"
        )
        database_integration = testing.Relation(
            endpoint=DATABASE_INTEGRATION_NAME,
            interface="mysql_client",
            remote_app_name="mysql",
            remote_app_data={
                "database": SLURM_ACCT_DATABASE_NAME,
                "endpoints": endpoints,
                "username": EXAMPLE_DATABASE_USERNAME,
                "password": EXAMPLE_DATABASE_PASSWORD,
            },
        )

        with mock_charm(
            mock_charm.on.relation_changed(database_integration),
            testing.State(
                leader=leader,
                relations={peer_integration, slurmctld_integration, database_integration},
            ),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            patch_slurmdbd_active(manager, mocker)
            mock_reconfigure = mocker.patch.object(slurmdbd, "reconfigure")
            mock_defer = mocker.patch.object(ops.EventBase, "defer")

            if leader:
                with pytest.raises(testing.errors.UncaughtCharmError) as excinfo:
                    manager.run()

                assert isinstance(excinfo.value.__cause__, ValueError)
                assert manager.charm.unit.status == ops.BlockedStatus(
                    "No database endpoints provided"
                )
            else:
                manager.run()

            mock_defer.assert_not_called()
            mock_reconfigure.assert_not_called()
            assert not slurmdbd.storage.path.exists()

    @pytest.mark.parametrize(
        "installed,joined,active,expected",
        (
            pytest.param(
                False,
                False,
                False,
                ops.BlockedStatus("`slurmdbd` is not installed. See `juju debug-log` for details"),
                id="not installed",
            ),
            pytest.param(
                True,
                False,
                False,
                ops.BlockedStatus("Waiting for integrations: [`slurmctld`, `database`]"),
                id="waiting for integrations",
            ),
            pytest.param(
                True,
                True,
                False,
                ops.WaitingStatus("Waiting for `slurmdbd` to start"),
                id="waiting for service",
            ),
            pytest.param(True, True, True, ops.ActiveStatus(), id="active"),
        ),
    )
    def test_update_status(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        installed,
        joined,
        active,
        expected,
        leader,
    ) -> None:
        """Test the status surface evaluated by `check_slurmdbd` after every handler.

        Failure mode: these statuses are what operators diagnose outages from. A
        wrong branch here misleads operators, e.g. showing "waiting" while the
        service is actually broken.
        """
        integrations = {peer_integration}
        if joined:
            integrations |= {
                testing.Relation(endpoint=SLURMDBD_INTEGRATION_NAME, interface="slurmdbd"),
                testing.Relation(endpoint=DATABASE_INTEGRATION_NAME, interface="mysql_client"),
            }

        with mock_charm(
            mock_charm.on.update_status(),
            testing.State(leader=leader, relations=integrations),
        ) as manager:
            slurmdbd = manager.charm.slurmdbd
            mocker.patch.object(slurmdbd, "is_installed", return_value=installed)
            mocker.patch.object(slurmdbd.service, "is_active", return_value=active)

            state = manager.run()

        # `_on_update_status` is guarded by `@leader`, so non-leader units never
        # evaluate the status hook and keep the status Juju assigned them.
        if leader:
            assert state.unit_status == expected
        else:
            assert state.unit_status == ops.UnknownStatus()
