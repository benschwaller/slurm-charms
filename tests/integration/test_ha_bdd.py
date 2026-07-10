# Copyright 2026 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BDD step definitions for slurmctld high availability.

Covers shared storage (MicroCeph + CephFS) deployment, scale up/down,
service and unit failover/recovery, degraded scale-up, and removal of a
failed controller. Gated by the ``high_availability`` marker
(``--run-high-availability``).
"""

import json
import logging
import subprocess

import jubilant
import pytest
from constants import (
    CEPHFS_SERVER_PROXY_APP_NAME,
    MICROCEPH_APP_NAME,
    SACKD_APP_NAME,
    SLURM_APPS,
    SLURM_WAIT_TIMEOUT,
    SLURMCTLD_APP_NAME,
)
from pytest_bdd import given, parsers, scenarios, then, when
from pytest_jubilant_bdd import Context

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.order(19),
    pytest.mark.high_availability,
]

scenarios("features/slurmctld_high_availability.feature")


# ---------------------------------------------------------------------------
# Slurm controller discovery via ``scontrol ping``
# ---------------------------------------------------------------------------


def _get_slurm_controllers(context: Context, query_unit: str = f"{SACKD_APP_NAME}/0") -> dict:
    """Return a dict of Slurmctld controller statuses keyed by mode.

    Polls ``scontrol ping --json`` on the login node and correlates ping
    results with ``juju status`` to map modes (primary, backup, ...) to
    unit, leader, and machine metadata.
    """
    juju = context.get_juju()
    status = juju.status()
    ping_output = json.loads(juju.exec("scontrol ping --json", unit=query_unit).stdout)
    pings = ping_output["pings"]
    pings_by_hostname = {ping["hostname"]: ping for ping in pings}

    slurm_controllers = {}
    for unit, unit_status in status.apps[SLURMCTLD_APP_NAME].units.items():
        hostname = status.machines[unit_status.machine].instance_id
        if hostname in pings_by_hostname:
            ping_data = pings_by_hostname[hostname]
            ping_data["unit"] = unit
            ping_data["leader"] = unit_status.leader
            ping_data["machine"] = unit_status.machine
            slurm_controllers[ping_data["mode"]] = ping_data

    return slurm_controllers


def _wait_for_controllers(context: Context, predicate) -> dict:
    """Poll ``_get_slurm_controllers`` until ``predicate(controllers)`` passes."""

    def ready(_ctx: Context) -> bool:
        try:
            controllers = _get_slurm_controllers(context)
            predicate(controllers)
            return True
        except AssertionError:
            return False

    context.wait(ready=ready)
    return _get_slurm_controllers(context)


# ---------------------------------------------------------------------------
# Deploy steps with constraints / storage / config
# ---------------------------------------------------------------------------


@given(
    parsers.parse("I deploy 'microceph' with constraints '{constraints}' and storage '{storage}'")
)
def deploy_microceph(context: Context, constraints: str, storage: str) -> None:
    """Deploy microceph with resource constraints and OSD storage."""
    juju = context.get_juju()

    constraints_dict = {}
    for pair in constraints.split(","):
        key, _, value = pair.partition("=")
        constraints_dict[key] = value

    storage_dict = {}
    for pair in storage.split(","):
        # Format: "osd-standalone=loop,2G,3"
        key, _, value = pair.partition("=")
        storage_dict[key] = value

    juju.deploy(
        MICROCEPH_APP_NAME,
        MICROCEPH_APP_NAME,
        constraints=constraints_dict,
        storage=storage_dict,
    )


@given(
    parsers.parse(
        "I deploy 'cephfs-server-proxy' from channel '{channel}' "
        "with cephfs config from unit 'microceph/0'"
    )
)
def deploy_cephfs_proxy(context: Context, channel: str) -> None:
    """Gather CephFS config from microceph and deploy cephfs-server-proxy."""
    juju = context.get_juju()
    microceph_unit = f"{MICROCEPH_APP_NAME}/0"

    microceph_host = juju.exec("hostname -I", unit=microceph_unit).stdout.strip()
    microceph_fsid = juju.exec(
        "microceph.ceph -s -f json | jq -r '.fsid'", unit=microceph_unit
    ).stdout.strip()
    microceph_key = juju.exec(
        "microceph.ceph auth print-key client.fs-client", unit=microceph_unit
    ).stdout

    juju.deploy(
        CEPHFS_SERVER_PROXY_APP_NAME,
        CEPHFS_SERVER_PROXY_APP_NAME,
        channel=channel,
        config={
            "fsid": microceph_fsid,
            "sharepoint": "cephfs:/",
            "monitor-hosts": microceph_host,
            "auth-info": f"fs-client:{microceph_key}",
        },
    )


# ---------------------------------------------------------------------------
# CephFS setup
# ---------------------------------------------------------------------------


@when(parsers.parse("I set up cephfs on unit '{unit}'"))
def setup_cephfs(context: Context, unit: str) -> None:
    """Create CephFS pools and authorise a client on microceph."""
    juju = context.get_juju()
    cephfs_setup = [
        "microceph.ceph osd pool create cephfs_data",
        "microceph.ceph osd pool create cephfs_metadata",
        "microceph.ceph fs new cephfs cephfs_metadata cephfs_data",
        "microceph.ceph fs authorize cephfs client.fs-client / rw",
    ]
    for cmd in cephfs_setup:
        juju.exec(cmd, unit=unit)


# ---------------------------------------------------------------------------
# Controller status assertions
# ---------------------------------------------------------------------------


@then(parsers.parse("the {mode} controller is '{status}'"))
def controller_status(context: Context, mode: str, status: str) -> None:
    """Assert that the controller in the given mode has the given pinged status."""

    def check(controllers):
        assert mode in controllers, f"controller mode '{mode}' not found"
        assert (
            controllers[mode]["pinged"] == status
        ), f"expected {mode} to be '{status}', got '{controllers[mode]['pinged']}'"

    _wait_for_controllers(context, check)


@then(parsers.parse("there are '{count}' slurm controllers"))
def controller_count(context: Context, count: str) -> None:
    """Assert the number of registered slurm controllers."""

    def check(controllers):
        assert len(controllers) == int(
            count
        ), f"expected {count} controllers, got {len(controllers)}"

    _wait_for_controllers(context, check)


# ---------------------------------------------------------------------------
# Scale up / down
# ---------------------------------------------------------------------------


@when(parsers.parse("I remove the '{mode}' controller unit"))
def remove_controller_unit(context: Context, mode: str) -> None:
    """Remove the slurmctld unit corresponding to the given controller mode."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    assert mode in controllers, f"controller mode '{mode}' not found"
    removed_unit = controllers[mode]["unit"]
    expected_units = len(juju.status().apps[SLURMCTLD_APP_NAME].units) - 1
    juju.remove_unit(removed_unit)
    juju.wait(
        lambda status: removed_unit not in status.apps[SLURMCTLD_APP_NAME].units
        and len(status.apps[SLURMCTLD_APP_NAME].units) == expected_units
        and jubilant.all_active(status, *SLURM_APPS),
        error=lambda status: jubilant.any_error(status, SLURMCTLD_APP_NAME),
        timeout=SLURM_WAIT_TIMEOUT,
    )


@when("I remove the down controller unit")
def remove_down_controller(context: Context) -> None:
    """Find and remove the powered-off slurmctld controller unit."""
    juju = context.get_juju()
    status = juju.status()
    down_unit = None
    for unit, unit_status in status.apps[SLURMCTLD_APP_NAME].units.items():
        if status.machines[unit_status.machine].juju_status.current == "down":
            down_unit = unit
            break
    assert down_unit is not None, "no down controller unit found"
    juju.remove_unit(down_unit, force=True)
    juju.wait(
        lambda status: jubilant.all_active(status, *SLURM_APPS),
        timeout=SLURM_WAIT_TIMEOUT,
    )


# ---------------------------------------------------------------------------
# Service failover / recovery
# ---------------------------------------------------------------------------


@when("I stop the primary controller service")
def stop_primary_service(context: Context) -> None:
    """Stop the slurmctld service on the primary controller unit."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]
    juju.exec(
        f"sudo systemctl stop {slurmctld_service}",
        unit=controllers["primary"]["unit"],
    )


@when("I restart the primary controller service")
def restart_primary_service(context: Context) -> None:
    """Restart the slurmctld service on the primary controller unit."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]
    juju.exec(
        f"sudo systemctl restart {slurmctld_service}",
        unit=controllers["primary"]["unit"],
    )


@then(parsers.parse("sinfo succeeds on unit '{unit}'"))
def sinfo_succeeds(context: Context, unit: str) -> None:
    """Poll until ``sinfo`` returns successfully on the given unit."""
    juju = context.get_juju()

    def ready(_ctx: Context) -> bool:
        result = juju.exec("sinfo", unit=unit, wait=30)
        return result.return_code == 0

    context.wait(ready=ready)


@then("the backup controller service is running as primary")
def backup_running_as_primary(context: Context) -> None:
    """Assert the backup controller's service shows 'Running as primary controller'."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

    def ready(_ctx: Context) -> bool:
        result = juju.exec(
            f"systemctl status {slurmctld_service}",
            unit=controllers["backup"]["unit"],
        )
        return "Running as primary controller" in result.stdout

    context.wait(ready=ready)


@then("the primary controller service is running as primary")
def primary_running_as_primary(context: Context) -> None:
    """Assert the primary controller's service shows 'Running as primary controller'."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

    def ready(_ctx: Context) -> bool:
        result = juju.exec(
            f"systemctl status {slurmctld_service}",
            unit=controllers["primary"]["unit"],
        )
        return "Running as primary controller" in result.stdout

    context.wait(ready=ready)


@then("the backup controller service is running in background mode")
def backup_running_in_background(context: Context) -> None:
    """Assert the backup controller's service shows 'running in background mode'."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    slurmctld_service = SLURM_APPS[SLURMCTLD_APP_NAME]

    def ready(_ctx: Context) -> bool:
        result = juju.exec(
            f"systemctl status {slurmctld_service}",
            unit=controllers["backup"]["unit"],
        )
        return "slurmctld running in background mode" in result.stdout

    context.wait(ready=ready)


# ---------------------------------------------------------------------------
# Machine power off / reboot
# ---------------------------------------------------------------------------


@when("I power off the primary controller machine")
def power_off_primary(context: Context) -> None:
    """Power off the primary controller's machine."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    juju.exec("sudo poweroff", unit=controllers["primary"]["unit"])
    machine_id = controllers["primary"]["machine"]
    juju.wait(
        lambda status: status.machines[machine_id].juju_status.current == "down",
        timeout=SLURM_WAIT_TIMEOUT,
    )


@then("the primary machine is powered off")
def primary_machine_off(context: Context) -> None:
    """Assert the primary controller's machine juju status is 'down'."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    machine_id = controllers["primary"]["machine"]
    assert juju.status().machines[machine_id].juju_status.current == "down"


@when("I reboot the primary controller machine")
def reboot_primary_machine(context: Context) -> None:
    """Start the powered-off primary machine via lxc."""
    juju = context.get_juju()
    controllers = _get_slurm_controllers(context)
    hostname = controllers["primary"]["hostname"]
    subprocess.check_output(["lxc", "start", hostname])
    juju.wait(
        lambda status: jubilant.all_active(status, SLURMCTLD_APP_NAME),
        timeout=SLURM_WAIT_TIMEOUT,
    )
