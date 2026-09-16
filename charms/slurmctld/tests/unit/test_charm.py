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

"""Unit tests for the `slurmctld` charmed operator."""

import json
import socket
import textwrap
from pathlib import Path
from unittest.mock import call

import ops
import pytest
from charmed_slurm_oci_runtime_interface import OCIRuntimeDisconnectedEvent, OCIRuntimeReadyEvent
from charmed_slurm_slurmctld_interface import AUTH_KEY_LABEL, JWT_KEY_LABEL
from charmlibs import apt
from config import ConfigData
from conftest import EXAMPLE_KEY_ENTRY, patch_slurmctld_active
from constants import (
    CLUSTER_NAME_PREFIX,
    HA_MOUNT_LOCATION,
    MAIL_INTEGRATION_NAME,
    OCI_RUNTIME_INTEGRATION_NAME,
    PEER_INTEGRATION_NAME,
    SLURMCTLD_PORT,
    SLURMD_INTEGRATION_NAME,
    SLURMDBD_INTEGRATION_NAME,
)
from ops import testing
from pydantic import ValidationError
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from slurm_ops import SlurmOpsError
from slurmutils import OCIConfig, Partition, SlurmConfig

EXAMPLE_OCI_CONFIG = OCIConfig(
    ignorefileconfigjson=True,
    envexclude="^(SLURM_CONF|SLURM_CONF_SERVER)=",
    runtimeenvexclude="^(SLURM_CONF|SLURM_CONF_SERVER)=",
    runtimerun=(
        "/usr/bin/apptainer exec --userns --bind /var/lib/slurm --bind /var/run/slurm %r %@"
    ),
    runtimekill="kill -s SIGTERM %p",
    runtimedelete="kill -s SIGKILL %p",
)
SLURM_CONF = Path("/etc/slurm/slurm.conf")
GRES_CONF = Path("/etc/slurm/gres.conf")
JWT_KEY_FILE = Path("/etc/slurm/jwt_hs256.key")
BASE_INCLUDES = ["slurm.conf.accounting", "slurm.conf.profiling", "slurm.conf.overrides"]


@pytest.fixture
def smtp_relation():
    """SMTP relation fixture."""
    return testing.Relation(
        endpoint=MAIL_INTEGRATION_NAME,
        interface="smtp",
        remote_app_name="smtp-integrator",
    )


@pytest.mark.parametrize(
    "leader",
    (
        pytest.param(True, id="leader"),
        pytest.param(False, id="not leader"),
    ),
)
class TestSlurmctldCharm:
    """Unit tests for the `slurmctld` charmed operator."""

    @pytest.mark.parametrize(
        "cluster_name",
        (
            pytest.param("polaris", id="cluster name configured"),
            pytest.param("", id="no cluster name configured"),
        ),
    )
    def test_on_slurmctld_peer_connected(
        self, mocker: MockerFixture, mock_charm, leader, cluster_name
    ) -> None:
        """Test the `_on_slurmctld_peer_connected` event handler."""
        # Patch `secrets.token_urlsafe(...)` to have predictable output.
        slug = "xyz1"
        mocker.patch("secrets.token_urlsafe", return_value=slug)

        peer_integration_id = 1
        peer_integration = testing.PeerRelation(
            endpoint=PEER_INTEGRATION_NAME,
            interface="slurmctld-peer",
            id=peer_integration_id,
        )

        state = mock_charm.run(
            mock_charm.on.relation_created(peer_integration),
            testing.State(
                leader=leader, relations={peer_integration}, config={"cluster-name": cluster_name}
            ),
        )

        integration = state.get_relation(peer_integration_id)
        if leader:
            assert "cluster_name" in integration.local_app_data
            if cluster_name:
                assert integration.local_app_data["cluster_name"] == '"polaris"'
            else:
                assert (
                    integration.local_app_data["cluster_name"] == f'"{CLUSTER_NAME_PREFIX}-{slug}"'
                )
        else:
            assert "cluster_name" not in integration.local_app_data

    @pytest.mark.parametrize(
        "ready",
        (
            pytest.param(True, id="ready"),
            pytest.param(False, id="not ready"),
        ),
    )
    def test_on_oci_runtime_ready(self, mock_charm, mocker: MockerFixture, ready, leader) -> None:
        """Test the `_on_oci_runtime_ready` event handler."""
        integration_id = 1
        integration = testing.Relation(
            endpoint=OCI_RUNTIME_INTEGRATION_NAME,
            interface="slurm-oci-runtime",
            id=integration_id,
            remote_app_name="apptainer",
            remote_app_data={"type": '"apptainer"', "executable_path": '"/usr/bin/apptainer"'}
            if ready
            else {},
        )

        with mock_charm(
            mock_charm.on.relation_changed(integration),
            testing.State(leader=leader, relations={integration}),
        ) as manager:
            slurmctld = manager.charm.slurmctld
            mocker.patch.object(slurmctld, "is_installed", return_value=True)

            manager.run()

        if ready and leader:
            assert slurmctld.oci.path.exists()
            assert slurmctld.oci.load().dict() == EXAMPLE_OCI_CONFIG.dict()
        else:
            assert not slurmctld.oci.path.exists()
            # Assert that `OCIRuntimeReadyEvent` is never emitted on non-leader units or
            # on the leader unit if `remote_app_data` is empty.
            assert not any(
                isinstance(event, OCIRuntimeReadyEvent) for event in mock_charm.emitted_events
            )

    def test_on_oci_runtime_disconnected(self, mock_charm, mocker: MockerFixture, leader) -> None:
        """Test the `_on_oci_runtime_disconnected` event handler."""
        integration_id = 1
        integration = testing.Relation(
            endpoint=OCI_RUNTIME_INTEGRATION_NAME,
            interface="slurm-oci-runtime",
            id=integration_id,
            remote_app_name="apptainer",
        )

        with mock_charm(
            mock_charm.on.relation_broken(integration),
            testing.State(leader=leader, relations={integration}),
        ) as manager:
            slurmctld = manager.charm.slurmctld
            mocker.patch.object(slurmctld, "is_installed", return_value=True)

            manager.run()

        if leader:
            assert not slurmctld.oci.path.exists()
        else:
            # Assert that `OCIRuntimeDisconnectedEvent` is only handled by the `slurmctld` leader.
            assert not any(
                isinstance(event, OCIRuntimeDisconnectedEvent)
                for event in mock_charm.emitted_events
            )

    def test_on_slurmd_node_departed_deletes_node(
        self, mock_charm, mocker: MockerFixture, leader
    ) -> None:
        """Test that `_on_slurmd_node_departed` deletes the departing compute node."""
        integration_id = 1
        integration = testing.Relation(
            endpoint=SLURMD_INTEGRATION_NAME,
            interface="slurmd",
            id=integration_id,
            remote_app_name="slurmd",
        )

        with mock_charm(
            mock_charm.on.relation_departed(integration, departing_unit=2),
            testing.State(leader=leader, relations={integration}),
        ) as manager:
            slurmctld = manager.charm.slurmctld
            mocker.patch.object(slurmctld, "is_installed", return_value=True)
            mock_delete = mocker.patch.object(slurmctld, "delete_compute_node")

            manager.run()

        if leader:
            mock_delete.assert_called_once_with("slurmd-2")
        else:
            mock_delete.assert_not_called()

    def test_bad_configuration(self, mock_charm, leader, peer_integration) -> None:
        """Test that a bad configuration blocks the ``_on_config_changed`` event handler."""
        state = mock_charm.run(
            mock_charm.on.config_changed(),
            testing.State(
                leader=leader,
                relations={peer_integration},
                config={"slurm-conf-parameters": "this is not valid slurm config="},
            ),
        )

        assert state.unit_status == ops.BlockedStatus(
            "Configuration option(s) 'slurm-conf-parameters' failed validation. "
            "See `juju debug-log` for details"
        )

    @pytest.mark.parametrize(
        "params,expected",
        (
            pytest.param(
                {"nodes": "slurmd-[0-19]", "state": "idle"},
                ("update", "nodename=slurmd-[0-19]", "state=idle"),
                id="idle",
            ),
            pytest.param(
                {"nodes": "slurmd-[20-24]", "state": "draining"},
                ("update", "nodename=slurmd-[20-24]", "state=draining", "reason='n/a'"),
                id="draining",
            ),
            pytest.param(
                {"nodes": "slurmd-[25-29]", "state": "down", "reason": "maintenance"},
                ("update", "nodename=slurmd-[25-29]", "state=down", "reason='maintenance'"),
                id="down",
            ),
        ),
    )
    def test_on_set_node_state_action(
        self, mock_charm, mock_scontrol, succeed, params, expected, leader
    ) -> None:
        """Test the `_on_set_node_state_action` action event handler."""
        if succeed:
            mock_charm.run(mock_charm.on.action("set-node-state", params=params), testing.State())
            mock_scontrol.assert_called_with(*expected)
        else:
            mock_scontrol.side_effect = [SlurmOpsError("scontrol failed")]
            with pytest.raises(testing.ActionFailed) as exec_info:
                mock_charm.run(
                    mock_charm.on.action("set-node-state", params=params), testing.State()
                )

            assert exec_info.value.message == (
                f"Failed to set state of node(s) {params['nodes']} to state '{params['state']}'. "
                f"reason:\nscontrol failed"
            )

    def test_on_smtp_relation_created_success(
        self, mock_charm, mocker: MockerFixture, leader, smtp_relation, peer_integration
    ) -> None:
        """Test successful integration on the SMTP interface."""
        mock_add_package = mocker.patch.object(apt, "add_package")
        mock_repo = mocker.patch.object(apt, "DebianRepository")
        mocker.patch.object(apt, "update")
        mocker.patch(
            "mail.platform.freedesktop_os_release",
            return_value={"VERSION_CODENAME": "noble"},
        )

        with mock_charm(
            mock_charm.on.relation_created(smtp_relation),
            testing.State(leader=leader, relations={smtp_relation, peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
            mocker.patch.object(manager.charm.slurmdbd, "is_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=EXAMPLE_KEY_ENTRY)
            state = manager.run()

        mock_add_package.assert_called_once_with("slurm-mail")
        assert mock_repo.call_args.kwargs["release"] == "noble"
        assert state.unit_status == testing.ActiveStatus()

    def test_on_smtp_relation_created_package_install_failure(
        self, mock_charm, mocker: MockerFixture, leader, smtp_relation, peer_integration
    ) -> None:
        """Test integration on the SMTP interface when package installation fails."""
        mock_add_package = mocker.patch.object(apt, "add_package")
        mock_add_package.side_effect = apt.PackageError()
        mocker.patch.object(apt, "DebianRepository")
        mocker.patch.object(apt, "update")
        mocker.patch(
            "mail.platform.freedesktop_os_release",
            return_value={"VERSION_CODENAME": "noble"},
        )

        with mock_charm(
            mock_charm.on.relation_created(smtp_relation),
            testing.State(leader=leader, relations={smtp_relation, peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
            mocker.patch.object(manager.charm.slurmdbd, "is_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=EXAMPLE_KEY_ENTRY)
            state = manager.run()

        assert state.unit_status == testing.BlockedStatus(
            "Failed to install slurm-mail package. See `juju debug-log` for details"
        )

    def test_on_smtp_relation_broken_success(
        self, mock_charm, mocker: MockerFixture, leader, smtp_relation, peer_integration
    ) -> None:
        """Test successful removal of integration on the SMTP interface."""
        mock_remove_package = mocker.patch.object(apt, "remove_package")

        with mock_charm(
            mock_charm.on.relation_broken(smtp_relation),
            testing.State(leader=leader, relations={smtp_relation, peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
            mocker.patch.object(manager.charm.slurmdbd, "is_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=EXAMPLE_KEY_ENTRY)
            state = manager.run()

        mock_remove_package.assert_called_once_with("slurm-mail")
        assert state.unit_status == testing.ActiveStatus()

    def test_on_smtp_relation_broken_failure(
        self, mock_charm, mocker: MockerFixture, leader, smtp_relation, peer_integration
    ) -> None:
        """Test integration on the SMTP interface when package removal fails."""
        mock_remove_package = mocker.patch.object(apt, "remove_package")
        mock_remove_package.side_effect = apt.PackageError()

        with mock_charm(
            mock_charm.on.relation_broken(smtp_relation),
            testing.State(leader=leader, relations={smtp_relation, peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
            mocker.patch.object(manager.charm.slurmdbd, "is_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=EXAMPLE_KEY_ENTRY)
            state = manager.run()

        assert state.unit_status == testing.BlockedStatus(
            "Failed to uninstall slurm-mail package. See `juju debug-log` for details"
        )

    def test_on_smtp_data_available(
        self, mock_charm, mocker: MockerFixture, leader, peer_integration
    ) -> None:
        """Test update of SMTP data."""
        password = "password%1234"
        secret = testing.Secret({"password": password}, owner="app")
        smtp_data = {
            "user": "myuser",
            "password_id": secret.id,
            "host": "smtp.example.com",
            "port": "1025",
            "auth_type": "none",
            "transport_security": "starttls",
        }

        smtp_integration = testing.Relation(
            endpoint=MAIL_INTEGRATION_NAME,
            interface="smtp",
            remote_app_name="smtp-integrator",
            remote_app_data=smtp_data,
        )

        # Initial slurm-mail.conf
        # Includes parameters to remain unchanged to confirm only relevant lines are updated
        initial_config_content = textwrap.dedent("""
            [slurm-send-mail]
            logFile = /var/log/slurm-mail/slurm-send-mail.log
            emailFromName = Charmed HPC Admin
            smtpServer = localhost
            smtpPort = 25
            smtpUseTls = no
            smtpUseSsl = no
            smtpUserName =
            smtpPassword =
            tailExe = /usr/bin/tail
        """)
        expected_config_content = textwrap.dedent(f"""
            [slurm-send-mail]
            logFile = /var/log/slurm-mail/slurm-send-mail.log
            emailFromName = Charmed HPC Admin
            smtpServer = {smtp_data["host"]}
            smtpPort = {smtp_data["port"]}
            smtpUseTls = yes
            smtpUseSsl = no
            smtpUserName = {smtp_data["user"]}
            smtpPassword = {password}
            tailExe = /usr/bin/tail
        """)
        config_path = Path("/etc/slurm-mail/slurm-mail.conf")
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(initial_config_content)

        with mock_charm(
            mock_charm.on.relation_changed(smtp_integration),
            testing.State(
                leader=leader, relations={smtp_integration, peer_integration}, secrets={secret}
            ),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
            mocker.patch.object(manager.charm.slurmdbd, "is_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=EXAMPLE_KEY_ENTRY)
            state = manager.run()

        assert config_path.read_text().strip() == expected_config_content.strip()
        assert state.unit_status == testing.ActiveStatus()

    @pytest.mark.parametrize(
        "damaged_content",
        (
            pytest.param("[not-slurm-send-mail]\nkey = value\n", id="wrong section"),
            pytest.param("key = value\n", id="no section header"),
        ),
    )
    def test_on_smtp_data_available_reinitializes_damaged_config(
        self, mock_charm, mocker: MockerFixture, leader, peer_integration, damaged_content
    ) -> None:
        """Test that a damaged `slurm-mail.conf` is reinitialized on SMTP data update.

        Failure mode: a corrupted configuration file (e.g. truncated by a
        crash) is missing the required `slurm-send-mail` section, and SMTP
        notifications silently never recover because the charm cannot
        update a section that does not exist.
        """
        password = "password%1234"
        secret = testing.Secret({"password": password}, owner="app")
        smtp_data = {
            "user": "myuser",
            "password_id": secret.id,
            "host": "smtp.example.com",
            "port": "1025",
            "auth_type": "none",
            "transport_security": "starttls",
        }

        smtp_integration = testing.Relation(
            endpoint=MAIL_INTEGRATION_NAME,
            interface="smtp",
            remote_app_name="smtp-integrator",
            remote_app_data=smtp_data,
        )

        # Damaged configuration file: the required section is missing.
        config_path = Path("/etc/slurm-mail/slurm-mail.conf")
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(damaged_content)

        with mock_charm(
            mock_charm.on.relation_changed(smtp_integration),
            testing.State(
                leader=leader, relations={smtp_integration, peer_integration}, secrets={secret}
            ),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
            mocker.patch.object(manager.charm.slurmdbd, "is_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=EXAMPLE_KEY_ENTRY)
            state = manager.run()

        content = config_path.read_text()
        assert "[slurm-send-mail]" in content
        assert "[not-slurm-send-mail]" not in content
        assert f"smtpServer = {smtp_data['host']}" in content
        assert state.unit_status == testing.ActiveStatus()


class TestConfigDataValidators:
    """Tests for the `ConfigData` pydantic validators."""

    def test_enable_configless_forced_true_when_override_disables_it(self) -> None:
        """An operator override that disables configless mode is overridden back to enabled.

        Failure mode: `enable_configless=False` in `slurm-conf-parameters` is
        accepted verbatim, silently disabling configless mode. Compute nodes
        then cannot fetch `slurm.conf` from the controller, which the charm
        source documents as cluster corruption.
        """
        config = ConfigData(
            cgroup_parameters="",
            cluster_name="charmed-hpc",
            default_partition="normal",
            email_from_name="Slurm Admin",
            slurm_conf_parameters="SlurmctldParameters=enable_configless=False",
        )

        assert config.slurm_conf_parameters.slurmctld_parameters["enable_configless"] is True

    def test_enable_configless_injected_when_override_sets_other_parameters(self) -> None:
        """`enable_configless` is added when an override sets other `SlurmctldParameters`.

        Failure mode: an override that sets unrelated `SlurmctldParameters`
        without mentioning `enable_configless` replaces the charm-managed
        parameter set, dropping configless mode and corrupting the cluster.
        """
        config = ConfigData(
            cgroup_parameters="",
            cluster_name="charmed-hpc",
            default_partition="normal",
            email_from_name="Slurm Admin",
            slurm_conf_parameters="SlurmctldParameters=idle_on_node_suspend=True",
        )

        parameters = config.slurm_conf_parameters.slurmctld_parameters
        assert parameters["idle_on_node_suspend"] is True
        assert parameters["enable_configless"] is True

    def test_cgroup_override_defaults_are_applied(self) -> None:
        """The charm's default cgroup constraints are applied on top of the override.

        Failure mode: an override that only sets a subset of cgroup options
        wipes the required defaults (`ConstrainCores`, `ConstrainRAMSpace`,
        etc.), leaving `cgroup.conf` under-constrained.
        """
        config = ConfigData(
            cgroup_parameters="ConstrainDevices=yes",
            cluster_name="charmed-hpc",
            default_partition="normal",
            email_from_name="Slurm Admin",
            slurm_conf_parameters="",
        )

        assert config.cgroup_parameters.constrain_devices is True
        assert config.cgroup_parameters.constrain_cores is True
        assert config.cgroup_parameters.constrain_ram_space is True
        assert config.cgroup_parameters.constrain_swap_space is True
        assert config.cgroup_parameters.signal_children_processes is True

    def test_invalid_cgroup_override_rejected(self) -> None:
        """A malformed `cgroup-conf-parameters` value is rejected, not applied.

        Failure mode: a typo'd cgroup directive is accepted and written to
        `cgroup.conf`, breaking `slurmctld` on the next service restart.
        """
        with pytest.raises(ValidationError, match="Invalid cgroup configuration"):
            ConfigData(
                cgroup_parameters="ThisIsNotAValidKey=yes",
                cluster_name="charmed-hpc",
                default_partition="normal",
                email_from_name="Slurm Admin",
                slurm_conf_parameters="",
            )

    def test_invalid_slurm_conf_override_rejected(self) -> None:
        """A malformed `slurm-conf-parameters` value is rejected, not applied.

        Failure mode: an unrecognized Slurm directive is accepted and written
        to the overrides include file, preventing `slurmctld` from starting.
        """
        with pytest.raises(ValidationError, match="Invalid slurm configuration override"):
            ConfigData(
                cgroup_parameters="",
                cluster_name="charmed-hpc",
                default_partition="normal",
                email_from_name="Slurm Admin",
                slurm_conf_parameters="NotARealSlurmDirective=5",
            )


class TestClusterNameImmutability:
    """Tests that the cluster name is written once and never overwritten."""

    def test_cluster_name_survives_restart_signal_update(
        self, mock_charm, peer_integration
    ) -> None:
        """A restart-signal-only peer databag update preserves the cluster name.

        `signal_slurmctld_restart` calls `update_controller_peer_app_data`
        with `cluster_name=None` on every reconfigure. Failure mode: a `None`
        cluster name overwrites the existing value, which the charm source
        documents as unrecoverable corruption of the controller's
        `StateSaveLocation` data.
        """
        with mock_charm(
            mock_charm.on.update_status(),
            testing.State(leader=True, relations={peer_integration}),
        ) as manager:
            peer = manager.charm.slurmctld_peer

            peer.signal_slurmctld_restart()

            data = peer.get_controller_peer_app_data()
            assert data is not None
            assert data.cluster_name == "charmed-hpc"
            assert data.restart_signal != ""

    def test_peer_connected_does_not_overwrite_existing_cluster_name(
        self, mock_charm, peer_integration
    ) -> None:
        """Re-emitting the peer connected event leaves an existing cluster name alone.

        Failure mode: a second `relation-created` (for example, from a
        re-deployment or peer churn) overwrites the cluster name, corrupting
        the controller's `StateSaveLocation` data.
        """
        state = mock_charm.run(
            mock_charm.on.relation_created(peer_integration),
            testing.State(
                leader=True,
                relations={peer_integration},
                # If the handler ignored the existing cluster name, it would
                # write this configured value instead.
                config={"cluster-name": "polaris"},
            ),
        )

        integration = state.get_relation(peer_integration.id)
        assert integration.local_app_data["cluster_name"] == '"charmed-hpc"'


class TestOnStart:
    """Tests for the `_on_start` event handler."""

    @pytest.mark.parametrize(
        "container,proctrack,task_plugin",
        (
            pytest.param(
                False, "proctrack/cgroup", ["task/cgroup", "task/affinity"], id="physical"
            ),
            pytest.param(True, "proctrack/linuxproc", ["task/affinity"], id="container"),
        ),
    )
    def test_on_start_writes_complete_slurm_conf_on_first_boot(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        container,
        proctrack,
        task_plugin,
    ) -> None:
        """The leader writes a complete, valid `slurm.conf` on first boot.

        Failure mode: `slurm.conf` is missing required directives (cluster
        name, controller port, configless mode) or selects plugins that cannot
        run in the deployment environment, preventing `slurmctld` from
        starting.
        """
        mocker.patch("charm.is_container", return_value=container)

        with mock_charm(
            mock_charm.on.start(),
            testing.State(leader=True, relations={peer_integration}),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_restart = mocker.patch.object(manager.charm.slurmctld.service, "restart")
            manager.run()

        config = SlurmConfig.from_str(SLURM_CONF.read_text())
        assert config.cluster_name == "charmed-hpc"
        assert config.slurmctld_port == SLURMCTLD_PORT
        assert config.slurmctld_parameters["enable_configless"] is True
        assert config.proctrack_type == proctrack
        assert config.task_plugin == task_plugin
        assert config.slurmctld_host == [socket.gethostname().split(".")[0]]
        assert config.include == BASE_INCLUDES

        # The include files must exist for `slurmctld` to start successfully.
        for include in config.include:
            assert (SLURM_CONF.parent / include).exists(), f"missing include file: {include}"

        assert GRES_CONF.exists()
        mock_restart.assert_called_once()

    def test_on_start_preserves_existing_config_on_reboot(
        self, mock_charm, mocker: MockerFixture, peer_integration, fs: FakeFilesystem
    ) -> None:
        """An existing `slurm.conf` and `gres.conf` are left untouched on re-run.

        The start hook executes again after a reboot of the underlying
        instance. Failure mode: the handler regenerates `slurm.conf`, wiping
        partition, accounting, and profiling includes that were assembled
        after the first boot.
        """
        existing_slurm_conf = (
            "clustername=charmed-hpc\ninclude=slurm.conf.accounting slurm.conf.compute\n"
        )
        existing_gres_conf = "autodetect=nvidia\n"
        fs.create_file(SLURM_CONF, contents=existing_slurm_conf)
        fs.create_file(GRES_CONF, contents=existing_gres_conf)

        with mock_charm(
            mock_charm.on.start(),
            testing.State(leader=True, relations={peer_integration}),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_restart = mocker.patch.object(manager.charm.slurmctld.service, "restart")
            manager.run()

        assert SLURM_CONF.read_text() == existing_slurm_conf
        assert GRES_CONF.read_text() == existing_gres_conf
        # The service is still restarted so `slurmctld` comes back up.
        mock_restart.assert_called_once()

    def test_on_start_failure_blocks_and_defers(
        self, mock_charm, mocker: MockerFixture, peer_integration
    ) -> None:
        """A `SlurmOpsError` during start blocks the unit and defers the event.

        Failure mode: the start failure is swallowed or the event is not
        deferred, leaving the unit stuck in a non-running state with no
        retry and no operator-visible status.
        """
        with mock_charm(
            mock_charm.on.start(),
            testing.State(leader=True, relations={peer_integration}),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mocker.patch("charm.is_container", return_value=False)
            mocker.patch.object(
                manager.charm.slurmctld.service,
                "restart",
                side_effect=SlurmOpsError("failed to restart slurmctld"),
            )
            state = manager.run()

        assert state.unit_status == ops.BlockedStatus(
            "Failed to start `slurmctld`. See `juju debug-log` for details"
        )
        assert any(event.name == "start" for event in state.deferred)

    def test_on_start_non_leader_does_not_write_config(
        self, mock_charm, mocker: MockerFixture, peer_integration, fs: FakeFilesystem
    ) -> None:
        """A non-leader unit never writes `slurm.conf`; it only starts the service.

        Failure mode: a non-leader writes its own `slurm.conf`, diverging
        from the leader's copy and corrupting the controller list that
        dictates primary/backup ordering.
        """
        hostname = socket.gethostname().split(".")[0]
        existing_slurm_conf = f"clustername=charmed-hpc\nslurmctldhost={hostname}\n"
        fs.create_file(SLURM_CONF, contents=existing_slurm_conf)
        fs.create_file(JWT_KEY_FILE, contents="dummy-jwt-key")
        # Non-leader units require the HA shared state filesystem to be mounted.
        fs.add_mount_point(HA_MOUNT_LOCATION)

        with mock_charm(
            mock_charm.on.start(),
            testing.State(leader=False, relations={peer_integration}),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_restart = mocker.patch.object(manager.charm.slurmctld.service, "restart")
            manager.run()

        assert SLURM_CONF.read_text() == existing_slurm_conf
        mock_restart.assert_called_once()


class TestSlurmIncludes:
    """Tests for `slurm.conf` include management across integration events."""

    @pytest.fixture
    def auth_key_secret(self) -> testing.Secret:
        """Application-owned auth key secret."""
        return testing.Secret(
            tracked_content={"key": "xyz123=="}, owner="app", label=AUTH_KEY_LABEL
        )

    @pytest.fixture
    def base_slurm_conf(self, fs: FakeFilesystem) -> None:
        """Create the `slurm.conf` state the leader leaves after `_on_start`."""
        fs.create_file(
            SLURM_CONF,
            contents=f"clustername=charmed-hpc\ninclude={' '.join(BASE_INCLUDES)}\n",
        )

    @pytest.fixture
    def slurmd_integration(self) -> testing.Relation:
        """`slurmd` integration with partition data in the remote application databag."""
        return testing.Relation(
            endpoint=SLURMD_INTEGRATION_NAME,
            interface="slurmd",
            remote_app_name="slurmd",
            remote_app_data={"partition": Partition(partitionname="compute*").json()},
        )

    @pytest.fixture
    def slurmdbd_integration(self) -> testing.Relation:
        """`slurmdbd` integration with database data in the remote application databag."""
        return testing.Relation(
            endpoint=SLURMDBD_INTEGRATION_NAME,
            interface="slurmdbd",
            remote_app_name="slurmdbd",
            remote_app_data={"hostname": json.dumps("slurmdbd-0")},
        )

    def test_on_slurmd_ready_does_not_duplicate_include(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        auth_key_secret,
        base_slurm_conf,
        slurmd_integration,
    ) -> None:
        """Repeated `slurmd` ready events converge to a single partition include.

        `relation-changed` fires many times in a real deployment. Failure
        mode: `Include slurm.conf.<partition>` lines accumulate in
        `slurm.conf` on every event, and duplicate partition definitions
        prevent `slurmctld` from starting.
        """
        state = testing.State(
            leader=True,
            relations={slurmd_integration, peer_integration},
            secrets={auth_key_secret},
        )

        for _ in range(2):
            with mock_charm(mock_charm.on.relation_changed(slurmd_integration), state) as manager:
                patch_slurmctld_active(manager, mocker)
                manager.run()

        config = SlurmConfig.from_str(SLURM_CONF.read_text())
        assert config.include.count("slurm.conf.compute*") == 1
        assert (SLURM_CONF.parent / "slurm.conf.compute*").exists()

    def test_on_slurmd_disconnected_removes_include_and_file(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        auth_key_secret,
        base_slurm_conf,
        slurmd_integration,
    ) -> None:
        """Disconnecting `slurmd` removes the partition include entry and file.

        Failure mode: a stale `Include` line pointing at a deleted file makes
        `scontrol reconfigure` fail, blocking reconfiguration cluster-wide.
        """
        state = testing.State(
            leader=True,
            relations={slurmd_integration, peer_integration},
            secrets={auth_key_secret},
        )

        with mock_charm(mock_charm.on.relation_changed(slurmd_integration), state) as manager:
            patch_slurmctld_active(manager, mocker)
            manager.run()
        with mock_charm(mock_charm.on.relation_broken(slurmd_integration), state) as manager:
            patch_slurmctld_active(manager, mocker)
            manager.run()

        config = SlurmConfig.from_str(SLURM_CONF.read_text())
        assert "slurm.conf.compute*" not in config.include
        assert not (SLURM_CONF.parent / "slurm.conf.compute*").exists()

    def test_on_slurmdbd_ready_does_not_duplicate_profiling_include(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        base_slurm_conf,
        slurmdbd_integration,
    ) -> None:
        """Repeated `slurmdbd` ready events keep a single profiling include.

        `_on_start` already lists `slurm.conf.profiling` in `slurm.conf`.
        Failure mode: `_on_slurmdbd_ready` prepends it again on every event,
        duplicating the include line in `slurm.conf`.
        """
        state = testing.State(leader=True, relations={slurmdbd_integration, peer_integration})

        for _ in range(2):
            with mock_charm(
                mock_charm.on.relation_changed(slurmdbd_integration), state
            ) as manager:
                patch_slurmctld_active(manager, mocker)
                manager.run()

        config = SlurmConfig.from_str(SLURM_CONF.read_text())
        assert config.include.count("slurm.conf.profiling") == 1

    def test_on_slurmdbd_disconnected_removes_profiling_include(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        base_slurm_conf,
        slurmdbd_integration,
    ) -> None:
        """Disconnecting `slurmdbd` removes the profiling include from `slurm.conf`.

        Failure mode: profiling configuration referencing a removed database
        left in `slurm.conf`, breaking accounting on the next restart.
        """
        state = testing.State(leader=True, relations={slurmdbd_integration, peer_integration})

        with mock_charm(mock_charm.on.relation_changed(slurmdbd_integration), state) as manager:
            patch_slurmctld_active(manager, mocker)
            manager.run()
        with mock_charm(mock_charm.on.relation_broken(slurmdbd_integration), state) as manager:
            patch_slurmctld_active(manager, mocker)
            manager.run()

        config = SlurmConfig.from_str(SLURM_CONF.read_text())
        assert "slurm.conf.profiling" not in config.include


class TestUnitStatus:
    """Tests for unit status evaluation via `check_slurmctld`.

    The `refresh` decorator runs `check_slurmctld` after every event handler,
    so `update_status` (whose handler is a no-op) surfaces the status
    precedence directly.
    """

    def test_status_blocked_takes_precedence_over_waiting(
        self, mock_charm, mocker: MockerFixture
    ) -> None:
        """A unit without `slurmctld` installed reports `BlockedStatus` even if nothing else is ready.

        Failure mode: the precedence order changes and the unit reports
        `WaitingStatus` for a unit that has no `slurmctld` installed, hiding
        the real problem from operators.
        """
        with mock_charm(mock_charm.on.update_status(), testing.State(leader=True)) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=False)
            state = manager.run()

        assert state.unit_status == ops.BlockedStatus(
            "`slurmctld` is not installed. See `juju debug-log` for details"
        )

    def test_status_waiting_for_cluster_name(self, mock_charm, mocker: MockerFixture) -> None:
        """An installed unit without peer data waits for the cluster name.

        Failure mode: the unit reports `ActiveStatus` or a misleading status
        before the cluster identity is established, masking a startup
        deadlock.
        """
        with mock_charm(mock_charm.on.update_status(), testing.State(leader=True)) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            state = manager.run()

        assert state.unit_status == ops.WaitingStatus("Waiting for the cluster name to be set")

    def test_status_waiting_when_service_inactive(
        self, mock_charm, mocker: MockerFixture, peer_integration
    ) -> None:
        """A unit whose `slurmctld` service is down waits for it to start.

        Failure mode: an inactive `slurmctld` service is reported as
        `ActiveStatus`, hiding an outage from operators and integration
        tests.
        """
        with mock_charm(
            mock_charm.on.update_status(),
            testing.State(leader=True, relations={peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=False)
            state = manager.run()

        assert state.unit_status == ops.WaitingStatus("Waiting for `slurmctld` to start")

    @pytest.mark.parametrize(
        "key_entry,expected_message",
        (
            pytest.param({"keys": []}, "Authentication key file contains no keys", id="no keys"),
            pytest.param(
                {"keys": [{"k": "1"}, {"k": "2"}]},
                "Authentication key rotation in progress. Waiting for rotation to complete",
                id="rotation in progress",
            ),
        ),
    )
    def test_status_waiting_on_auth_key_state(
        self, mock_charm, mocker: MockerFixture, peer_integration, key_entry, expected_message
    ) -> None:
        """The unit stays in `WaitingStatus` while the auth key is empty or mid-rotation.

        Failure mode: a unit with zero keys or a half-finished key rotation
        reports `ActiveStatus`, so a broken or in-progress rotation is never
        surfaced and never completes.
        """
        with mock_charm(
            mock_charm.on.update_status(),
            testing.State(leader=True, relations={peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.key, "get", return_value=key_entry)
            state = manager.run()

        assert state.unit_status == ops.WaitingStatus(expected_message)


class TestPeerReadiness:
    """Tests for the conditions that gate a non-leader unit from starting."""

    @pytest.mark.parametrize(
        "missing,expected_message",
        (
            pytest.param(
                "slurm.conf",
                "Waiting for /etc/slurm/slurm.conf",
                id="no slurm.conf",
            ),
            pytest.param(
                "hostname",
                (
                    f"Waiting for {socket.gethostname().split('.')[0]} "
                    "to be added to /etc/slurm/slurm.conf"
                ),
                id="hostname not in slurm.conf",
            ),
            pytest.param(
                "jwt key",
                "Waiting for /etc/slurm/jwt_hs256.key",
                id="no jwt key",
            ),
            pytest.param(
                "auth key",
                "Waiting for /etc/slurm/slurm.jwks",
                id="no auth key",
            ),
        ),
    )
    def test_on_start_non_leader_waits_for_missing_prerequisites(
        self,
        mock_charm,
        mocker: MockerFixture,
        peer_integration,
        fs: FakeFilesystem,
        missing,
        expected_message,
    ) -> None:
        """A non-leader reports exactly which prerequisite it is waiting for.

        Failure mode: a newly added controller unit waits forever with a
        generic status, leaving the operator without a clue which handoff
        from the leader is missing.
        """
        fs.add_mount_point(HA_MOUNT_LOCATION)
        hostname = socket.gethostname().split(".")[0]

        if missing == "hostname":
            fs.create_file(
                SLURM_CONF, contents="clustername=charmed-hpc\nslurmctldhost=other-controller\n"
            )
        elif missing == "jwt key":
            fs.create_file(
                SLURM_CONF, contents=f"clustername=charmed-hpc\nslurmctldhost={hostname}\n"
            )
        elif missing == "auth key":
            fs.create_file(
                SLURM_CONF, contents=f"clustername=charmed-hpc\nslurmctldhost={hostname}\n"
            )
            fs.create_file(JWT_KEY_FILE, contents="dummy-jwt-key")
            Path("/etc/slurm/slurm.jwks").unlink()

        with mock_charm(
            mock_charm.on.start(),
            testing.State(leader=False, relations={peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            state = manager.run()

        assert state.unit_status == ops.WaitingStatus(expected_message)
        assert any(event.name == "start" for event in state.deferred)

    def test_on_start_non_leader_blocks_without_shared_state(
        self, mock_charm, mocker: MockerFixture, peer_integration
    ) -> None:
        """A non-leader refuses to start without the HA shared state filesystem.

        Failure mode: a backup controller starts with unshared state,
        silently diverging from the primary's checkpoint data.
        """
        with mock_charm(
            mock_charm.on.start(),
            testing.State(leader=False, relations={peer_integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            state = manager.run()

        assert state.unit_status == ops.BlockedStatus(
            "A shared file system must be provided to enable `slurmctld` high availability"
        )


class TestReconfigure:
    """Tests for the `_reconfigure` orchestration."""

    @pytest.fixture
    def ha_peer_integration(self) -> testing.PeerRelation:
        """Peer integration with a second controller unit, as in an HA deployment."""
        return testing.PeerRelation(
            endpoint=PEER_INTEGRATION_NAME,
            interface="slurmctld-peer",
            local_app_data={"cluster_name": '"charmed-hpc"'},
            peers_data={1: {"hostname": '"controller-1"'}},
        )

    def test_reconfigure_restarts_before_scontrol_reconfigure(
        self, mock_charm, mocker: MockerFixture, ha_peer_integration, fs: FakeFilesystem
    ) -> None:
        """`slurmctld` is restarted before `scontrol reconfigure` runs.

        The handler's docstring records the failure mode of getting this
        backwards: `scontrol reconfigure` fails with "Slurm backup controller
        in standby mode" when a backup is being promoted to primary.
        """
        fs.create_file(SLURM_CONF, contents="clustername=charmed-hpc\n")

        with mock_charm(
            mock_charm.on.relation_changed(ha_peer_integration, remote_unit=1),
            testing.State(leader=True, relations={ha_peer_integration}, planned_units=2),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_reconfigure = mocker.patch.object(manager.charm.slurmctld, "reconfigure")
            manager.run()

        assert mock_reconfigure.call_args_list == [call(restart=True), call()]

    def test_reconfigure_noop_when_slurmctld_not_ready(
        self, mock_charm, mocker: MockerFixture, ha_peer_integration, fs: FakeFilesystem
    ) -> None:
        """`_reconfigure` does nothing while `slurmctld` is not running.

        Failure mode: reconfiguration is attempted on a stopped service,
        restarting it out of order and leaving the cluster in an
        inconsistent state.
        """
        fs.create_file(SLURM_CONF, contents="clustername=charmed-hpc\n")

        with mock_charm(
            mock_charm.on.relation_changed(ha_peer_integration, remote_unit=1),
            testing.State(leader=True, relations={ha_peer_integration}, planned_units=2),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(manager.charm.slurmctld.service, "is_active", return_value=False)
            mock_reconfigure = mocker.patch.object(manager.charm.slurmctld, "reconfigure")
            manager.run()

        mock_reconfigure.assert_not_called()

    def test_reconfigure_restart_failure_blocks(
        self, mock_charm, mocker: MockerFixture, ha_peer_integration, fs: FakeFilesystem
    ) -> None:
        """A failed service restart during reconfiguration blocks the unit.

        Failure mode: the restart failure is swallowed and `scontrol
        reconfigure` proceeds against a dead service, or peers are signalled
        to restart a service that never came back up.
        """
        fs.create_file(SLURM_CONF, contents="clustername=charmed-hpc\n")

        with mock_charm(
            mock_charm.on.relation_changed(ha_peer_integration, remote_unit=1),
            testing.State(leader=True, relations={ha_peer_integration}, planned_units=2),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mocker.patch.object(
                manager.charm.slurmctld, "reconfigure", side_effect=SlurmOpsError("restart failed")
            )
            state = manager.run()

        assert state.unit_status == ops.BlockedStatus(
            "Failed to restart `slurmctld.service`. See `juju debug-log` for details"
        )
        # The restart signal is only sent after a successful restart.
        integration = state.get_relation(ha_peer_integration.id)
        assert "restart_signal" not in integration.local_app_data

    def test_reconfigure_scontrol_failure_blocks(
        self, mock_charm, mocker: MockerFixture, ha_peer_integration, fs: FakeFilesystem
    ) -> None:
        """A failed `scontrol reconfigure` blocks the unit.

        Failure mode: the configuration failure is swallowed, leaving
        `slurm.conf` changes unapplied while the unit reports healthy.
        """
        fs.create_file(SLURM_CONF, contents="clustername=charmed-hpc\n")

        with mock_charm(
            mock_charm.on.relation_changed(ha_peer_integration, remote_unit=1),
            testing.State(leader=True, relations={ha_peer_integration}, planned_units=2),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mocker.patch.object(
                manager.charm.slurmctld,
                "reconfigure",
                side_effect=[None, SlurmOpsError("scontrol failed")],
            )
            state = manager.run()

        assert state.unit_status == ops.BlockedStatus(
            "Failed to apply new Slurm configuration. See `juju debug-log` for details"
        )
        # Peers were already signalled to restart before `scontrol reconfigure` ran.
        integration = state.get_relation(ha_peer_integration.id)
        assert "restart_signal" in integration.local_app_data


class TestSmtpScaleDown:
    """Tests for SMTP cleanup behavior when this unit is scaled down."""

    def test_on_smtp_relation_broken_scale_down_skips_mail_uninstall(
        self, mock_charm, mocker: MockerFixture, smtp_relation, peer_integration
    ) -> None:
        """Mail is not uninstalled when the relation breaks due to this unit scaling down.

        In an HA deployment `/etc/slurm` is a symlink to shared storage, so
        cleanup by a departing unit would remove `mail_prog` from the
        `slurm.conf` that the remaining units still need. Failure mode: the
        departing unit runs the full uninstall path during scale-down.
        """
        with mock_charm(
            mock_charm.on.relation_broken(smtp_relation),
            testing.State(leader=True, relations={smtp_relation, peer_integration}),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_remove = mocker.patch.object(apt, "remove_package")
            # Scenario cannot express the local unit as the departing unit of a
            # relation (`JUJU_DEPARTING_UNIT` is always remote), so drive the
            # handler directly with the local unit, as Juju does during scale-down.
            departing = mocker.Mock(departing_unit=manager.charm.unit)
            manager.charm._on_smtp_relation_departed(departing)
            manager.run()

        mock_remove.assert_not_called()


class TestOnInstall:
    """Tests for the `_on_install` event handler."""

    @pytest.mark.parametrize(
        "leader",
        (
            pytest.param(True, id="leader"),
            pytest.param(False, id="not leader"),
        ),
    )
    def test_on_install_generates_keys_when_no_secrets_exist(
        self, mock_charm, mocker: MockerFixture, leader
    ) -> None:
        """The leader generates and publishes both keys when no secrets exist.

        Failure mode: keys are not created and `slurmctld` cannot start, or a
        non-leader generates keys, racing the leader and clobbering the
        cluster's auth material.
        """
        with mock_charm(mock_charm.on.install(), testing.State(leader=leader)) as manager:
            mocker.patch.object(manager.charm.slurmctld, "install")
            mocker.patch.object(manager.charm.slurmctld, "version", return_value="24.05.2")
            mocker.patch.object(
                manager.charm.slurmctld.jwt, "generate", return_value={"key": "jwt-key"}
            )
            mocker.patch.object(
                manager.charm.slurmctld.key,
                "generate",
                return_value={"key": "auth-key", "keyid": "keyid-1"},
            )
            state = manager.run()

        labels = {secret.label for secret in state.secrets}
        if leader:
            assert JWT_KEY_FILE.read_text() == "jwt-key"
            assert "auth-key" in Path("/etc/slurm/slurm.jwks").read_text()
            assert {AUTH_KEY_LABEL, JWT_KEY_LABEL} <= labels
        else:
            # Only the leader may generate and publish keys.
            assert not JWT_KEY_FILE.exists()
            assert AUTH_KEY_LABEL not in labels
            assert JWT_KEY_LABEL not in labels

    def test_on_install_restores_missing_key_file_from_secret(
        self, mock_charm, mocker: MockerFixture
    ) -> None:
        """An existing secret is the source of truth: a missing key file is restored, not regenerated.

        Failure mode: a new leader regenerates keys instead of restoring
        them, invalidating the cluster's auth material shared with every
        other Slurm service.
        """
        jwt_secret = testing.Secret({"key": "restored-jwt"}, owner="app", label=JWT_KEY_LABEL)
        auth_secret = testing.Secret(
            {"key": "restored-auth", "keyid": "keyid-1"}, owner="app", label=AUTH_KEY_LABEL
        )
        # The auth key file exists (created by the test fixture); the JWT key file does not.

        with mock_charm(
            mock_charm.on.install(),
            testing.State(leader=True, secrets={jwt_secret, auth_secret}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "install")
            mocker.patch.object(manager.charm.slurmctld, "version", return_value="24.05.2")
            mock_jwt_generate = mocker.patch.object(manager.charm.slurmctld.jwt, "generate")
            mock_key_generate = mocker.patch.object(manager.charm.slurmctld.key, "generate")
            manager.run()

        assert JWT_KEY_FILE.read_text() == "restored-jwt"
        # The existing auth key file is not clobbered.
        assert Path("/etc/slurm/slurm.jwks").read_text() == '{"keys": []}'
        mock_jwt_generate.assert_not_called()
        mock_key_generate.assert_not_called()

    def test_on_install_failure_blocks_and_defers(self, mock_charm, mocker: MockerFixture) -> None:
        """A `SlurmOpsError` during install blocks the unit and defers the event.

        Failure mode: an install failure is swallowed or never retried,
        leaving the unit in a non-running state with no operator-visible
        status.
        """
        with mock_charm(mock_charm.on.install(), testing.State(leader=True)) as manager:
            mocker.patch.object(
                manager.charm.slurmctld, "install", side_effect=SlurmOpsError("install failed")
            )
            state = manager.run()

        assert state.unit_status == ops.BlockedStatus(
            "Failed to install `slurmctld`. See `juju debug-log` for details."
        )
        assert any(event.name == "install" for event in state.deferred)


class TestKeyRotation:
    """Tests for the `rotate-auth-key` and `rotate-jwt-key` action guards and failures."""

    @pytest.mark.parametrize(
        "action,name",
        (
            pytest.param("rotate-auth-key", "auth", id="rotate auth key"),
            pytest.param("rotate-jwt-key", "JWT", id="rotate jwt key"),
        ),
    )
    def test_rotate_key_action_fails_on_non_leader(self, mock_charm, action, name) -> None:
        """Key rotation is rejected on non-leader units.

        Failure mode: a non-leader rotates keys, desynchronizing the
        cluster's auth material from the Juju secret.
        """
        with pytest.raises(testing.ActionFailed) as exec_info:
            mock_charm.run(mock_charm.on.action(action), testing.State(leader=False))

        assert exec_info.value.message == f"Only the leader unit can rotate the {name} key."

    def test_rotate_auth_key_apply_failure_fails_action(
        self, mock_charm, mocker: MockerFixture
    ) -> None:
        """A failure applying the new key fails the action with an operator-visible message.

        Failure mode: the key file is left inconsistent with the Juju
        secret while the action reports success.
        """
        with mock_charm(
            mock_charm.on.action("rotate-auth-key"), testing.State(leader=True)
        ) as manager:
            mocker.patch.object(
                manager.charm.slurmctld.key,
                "generate",
                return_value={"key": "new-key", "keyid": "keyid-2"},
            )
            mocker.patch.object(
                manager.charm.slurmctld.key,
                "apply",
                side_effect=SlurmOpsError("apply failed"),
            )
            with pytest.raises(testing.ActionFailed) as exec_info:
                manager.run()

        assert exec_info.value.message == (
            "Failed to update auth key. See `juju debug-log` for details."
        )

    def test_rotate_auth_key_publish_failure_fails_action(
        self, mock_charm, mocker: MockerFixture, peer_integration
    ) -> None:
        """A failure publishing the new key secret fails the action.

        Failure mode: the key file is rotated but the Juju secret is not,
        so peer units never receive the new key and authentication breaks.
        """
        with mock_charm(
            mock_charm.on.action("rotate-auth-key"),
            testing.State(leader=True, relations={peer_integration}),
        ) as manager:
            mocker.patch.object(
                manager.charm.slurmctld.key,
                "generate",
                return_value={"key": "new-key", "keyid": "keyid-2"},
            )
            mocker.patch.object(manager.charm.slurmctld.key, "apply")
            mocker.patch("charm.slurmctld_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld, "reconfigure")
            # No secret with the auth key label exists in the state, so publishing fails.
            with pytest.raises(testing.ActionFailed) as exec_info:
                manager.run()

        assert exec_info.value.message == (
            "Failed to publish new auth key secret. See `juju debug-log` for details."
        )


class TestSecretLifecycle:
    """Tests for the secret-changed and secret-remove event handlers."""

    def test_on_secret_remove_auth_key_keeps_latest_key(
        self, mock_charm, mocker: MockerFixture, peer_integration
    ) -> None:
        """Removing an old auth key revision after rotation keeps only the latest key.

        This is the completion step of key rotation: the old revision is
        only removed once a newer one exists. Failure mode: the old key is
        never cleaned up and the unit stays in `WaitingStatus` ("rotation
        in progress") forever.
        """
        auth_secret = testing.Secret(
            {"key": "auth-key", "keyid": "keyid-1"}, owner="app", label=AUTH_KEY_LABEL
        )

        # Rotate the key first: Juju only fires `secret-remove` for old revisions.
        with mock_charm(
            mock_charm.on.action("rotate-auth-key"),
            testing.State(leader=True, relations={peer_integration}, secrets={auth_secret}),
        ) as manager:
            mocker.patch.object(
                manager.charm.slurmctld.key,
                "generate",
                return_value={"key": "new-key", "keyid": "keyid-2"},
            )
            mocker.patch.object(manager.charm.slurmctld.key, "apply")
            mocker.patch("charm.slurmctld_ready", return_value=True)
            mocker.patch.object(manager.charm.slurmctld, "reconfigure")
            state = manager.run()

        rotated = next(secret for secret in state.secrets if secret.label == AUTH_KEY_LABEL)
        with mock_charm(
            mock_charm.on.secret_remove(rotated, revision=1),
            testing.State(leader=True, relations={peer_integration}, secrets={rotated}),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_keep = mocker.patch.object(manager.charm.slurmctld.key, "keep_latest_key")
            mocker.patch.object(manager.charm.slurmctld, "reconfigure")
            # Scenario cannot fire `secret-changed` for an app-owned secret, so
            # advance the tracked revision directly, as `_on_secret_changed`
            # does when Juju notifies the owner of the new revision.
            manager.charm.model.get_secret(label=AUTH_KEY_LABEL).get_content(refresh=True)
            manager.run()

        mock_keep.assert_called_once()

    def test_on_secret_remove_jwt_key_is_ignored(self, mock_charm, mocker: MockerFixture) -> None:
        """Removing a JWT key revision does not trigger auth key cleanup.

        Failure mode: JWT rotation triggers `keep_latest_key` on the auth
        key mid-rotation, breaking cluster authentication.
        """
        jwt_secret = testing.Secret({"key": "jwt-key"}, owner="app", label=JWT_KEY_LABEL)
        with mock_charm(
            mock_charm.on.secret_remove(jwt_secret, revision=1),
            testing.State(leader=True, secrets={jwt_secret}),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_keep = mocker.patch.object(manager.charm.slurmctld.key, "keep_latest_key")
            manager.run()

        mock_keep.assert_not_called()

    @pytest.mark.parametrize(
        "label,expected_tracked",
        (
            pytest.param(AUTH_KEY_LABEL, "new-key", id="auth key revision refreshed"),
            pytest.param("unrelated-secret", "old-key", id="unrelated secret ignored"),
        ),
    )
    def test_on_secret_changed_label_filter(
        self, mock_charm, mocker: MockerFixture, label, expected_tracked
    ) -> None:
        """Only auth/JWT secret changes refresh the tracked revision.

        Failure mode: every secret change forces revision tracking,
        breaking the secret-remove bookkeeping that key rotation relies on.
        """
        secret = testing.Secret(
            tracked_content={"key": "old-key"},
            latest_content={"key": "new-key"},
            label=label,
        )
        with mock_charm(
            mock_charm.on.secret_changed(secret),
            testing.State(leader=True, secrets={secret}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            state = manager.run()

        updated = next(secret for secret in state.secrets if secret.label == label)
        assert updated.tracked_content["key"] == expected_tracked


class TestRestartSignal:
    """Tests for the peer restart-signal propagation mechanism."""

    @pytest.mark.parametrize(
        "leader",
        (
            pytest.param(True, id="leader"),
            pytest.param(False, id="not leader"),
        ),
    )
    def test_restart_signal_restarts_non_leader_only(
        self, mock_charm, mocker: MockerFixture, fs: FakeFilesystem, leader
    ) -> None:
        """A restart signal restarts `slurmctld` on non-leader units only.

        The leader restarts its own service as part of `_reconfigure`;
        non-leaders act on the signal. Failure mode: backup controllers
        never pick up `slurm.conf` changes, or the leader restarts twice.
        """
        fs.add_mount_point(HA_MOUNT_LOCATION)
        hostname = socket.gethostname().split(".")[0]
        fs.create_file(SLURM_CONF, contents=f"clustername=charmed-hpc\nslurmctldhost={hostname}\n")
        fs.create_file(JWT_KEY_FILE, contents="dummy-jwt-key")

        peer = testing.PeerRelation(
            endpoint=PEER_INTEGRATION_NAME,
            interface="slurmctld-peer",
            local_app_data={"cluster_name": '"charmed-hpc"', "restart_signal": '"signal-1"'},
            peers_data={1: {"hostname": '"controller-1"'}},
        )

        with mock_charm(
            mock_charm.on.relation_changed(peer, remote_unit=1),
            testing.State(leader=leader, relations={peer}, planned_units=2),
        ) as manager:
            patch_slurmctld_active(manager, mocker)
            mock_restart = mocker.patch.object(manager.charm.slurmctld.service, "restart")
            manager.run()

        if leader:
            # The leader restarts within `_reconfigure`, not on the signal.
            mock_restart.assert_not_called()
        else:
            mock_restart.assert_called_once()


class TestSlurmdNodeDeparture:
    """Tests for the `_on_slurmd_node_departed` event handler failure path."""

    def test_on_slurmd_node_departed_delete_failure_blocks_and_defers(
        self, mock_charm, mocker: MockerFixture
    ) -> None:
        """A failed node deletion blocks the unit and defers the event.

        Failure mode: the departure is silently dropped and the node remains
        registered in Slurm, poisoning scheduling.
        """
        integration = testing.Relation(
            endpoint=SLURMD_INTEGRATION_NAME,
            interface="slurmd",
            remote_app_name="slurmd",
        )
        with mock_charm(
            mock_charm.on.relation_departed(integration, departing_unit=2),
            testing.State(leader=True, relations={integration}),
        ) as manager:
            mocker.patch.object(manager.charm.slurmctld, "is_installed", return_value=True)
            mocker.patch.object(
                manager.charm.slurmctld,
                "delete_compute_node",
                side_effect=SlurmOpsError("delete failed"),
            )
            state = manager.run()

        assert state.unit_status == ops.BlockedStatus(
            "Failed to delete departing compute node `slurmd-2` from Slurm. "
            "See `juju debug-log` for details"
        )
        assert any("departed" in event.name for event in state.deferred)
